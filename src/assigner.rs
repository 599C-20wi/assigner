use std::cmp;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};
use std::{thread, time};

extern crate mysql;

use crate::message::Update;
use crate::types::Slice;
use crate::utils;


#[derive(Debug, PartialEq, Eq)]
struct RowCount {
    slice_key: String,
    results: i32,
}

#[derive(Debug)]
enum MoveType {
    Reassign,
    Duplicate,
    Remove,
}

#[derive(Debug)]
struct Move {
    move_type: MoveType,
    source: String,
    destination: String,
    slice: Slice,
    weight: i32,
    key_churn: i32,
}

pub fn run(counter: Arc<RwLock<HashMap<&'static str, Vec<Slice>>>>) {
    thread::spawn(move || {
        start_loop(counter);
    });
}

fn start_loop(counter: Arc<RwLock<HashMap<&str, Vec<Slice>>>>) {
    let username =
        std::env::var("RDS_USERNAME").expect("expected RDS_USERNAME environment variable");
    let password =
        std::env::var("RDS_PASSWORD").expect("expected RDS_PASSWORD environment variable");
    let host = std::env::var("RDS_HOST").expect("expected RDS_HOST environment variable");
    let port = std::env::var("RDS_PORT").expect("expected RDS_PORT environment variable");

    let url = format!("mysql://{}:{}@{}:{}/slicer", username, password, host, port);
    let pool = mysql::Pool::new(url.as_str()).unwrap();

    let sql = "
        SELECT
            slice_key, COUNT(*)
        FROM
            expressions
        WHERE DATE_ADD(timestamp, INTERVAL 5 SECOND) >= NOW()
        GROUP BY slice_key";
    // WHERE (timestamp BETWEEN '2020-02-26 22:14:59' AND '2020-02-26 22:15:04')

    loop {
        trace!("generating assignments");
        let mut assignments = counter.write().unwrap();
        let rows: Vec<RowCount> = pool
            .prep_exec(sql, ())
            .map(|result| {
                result
                    .map(|x| x.unwrap())
                    .map(|row| {
                        let (slice_key, results) = mysql::from_row(row);
                        RowCount { slice_key, results }
                    })
                    .collect()
            })
            .unwrap();

        let slice_load = slice_load(&rows, &assignments);
        trace!("slice load: {:?}", slice_load);

        let moves = get_moves(&assignments, &slice_load);
        let mut moves: Vec<Move> = moves.into_iter().filter(|x| x.weight > 0).collect();
        moves.sort_by(|a, b| b.weight.cmp(&a.weight));
        debug!("moves: {:?}", moves);

        apply_move(&mut assignments, &moves[0]);

        for row in rows {
            let slice_key = row.slice_key;
            let count = row.results;

            trace!("{} processed {} queries", slice_key, count);
        }

        thread::sleep(time::Duration::from_millis(5000));
    }
}

fn slices(assignments: &HashMap<&str, Vec<Slice>>) -> HashSet<Slice> {
    let mut set = HashSet::new();
    for slices in assignments.values() {
        for slice in slices {
            set.insert(slice.clone());
        }
    }
    set
}

// Return a map of slice to the number of requests for that slice.
fn slice_load(
    rows: &Vec<RowCount>,
    assignments: &HashMap<&str, Vec<Slice>>,
) -> HashMap<Slice, i64> {
    let mut map: HashMap<Slice, i64> = HashMap::new();

    let slices = slices(assignments);
    for slice in slices {
        map.insert(slice, 0);
        for row in rows {
            let slice_key = &row.slice_key.parse::<u64>().unwrap();
            let count = row.results as i64;

            if slice_key >= &slice.start && slice_key <= &slice.end {
                *map.get_mut(&slice).unwrap() += count;
            }
        }
    }

    map
}

// Returns the load imbalance for the current assignments and slice load.
fn load_imbalance(
    current_assignments: &HashMap<&str, Vec<Slice>>,
    slice_load: &HashMap<Slice, i64>,
) -> f32 {
    let mut max_load = 0;
    let mut mean_load = 0;

    let mut slice_task_count: HashMap<Slice, i64> = HashMap::new();
    for &slice in slice_load.keys() {
        // How many servers contain this slice.
        let mut servers = 0;
        for slices in current_assignments.values() {
            if slices.contains(&slice) {
                servers += 1;
            }
        }
        // *slice_task_count.get_mut(&slice).unwrap() = servers;
        // slice_task_count[&slice] = servers;
        slice_task_count.insert(slice, servers);
    }

    let mut task_load: HashMap<&str, i64> = HashMap::new();
    for (task, slices) in current_assignments.iter() {
        task_load.insert(task, 0);
        for slice in slices {
            *task_load.get_mut(task).unwrap() += slice_load[slice] / slice_task_count[slice]
        }
    }

    for count in task_load.values() {
        max_load = cmp::max(max_load, *count);
        mean_load += count;
    }
    mean_load /= current_assignments.len() as i64;

    if mean_load == 0 {
        return 0.0;
    }
    debug!("max load={}, mean load={}", max_load, mean_load);
    (max_load as f32) / (mean_load as f32)
}

fn get_moves(
    current_assignments: &HashMap<&str, Vec<Slice>>,
    slice_load: &HashMap<Slice, i64>,
) -> Vec<Move> {
    let mut moves = Vec::new();

    let load_imbalance = load_imbalance(current_assignments, slice_load);

    for (task, slices) in current_assignments.iter() {
        // For each slice in the task...
        for slice in slices {
            // What can we do with the slice?
            moves.append(&mut get_reassign_moves(
                &task,
                &slice,
                load_imbalance,
                &current_assignments,
                &slice_load,
            ));
            moves.append(&mut get_duplicate_moves(
                &task,
                &slice,
                load_imbalance,
                &current_assignments,
                &slice_load,
            ));
            moves.append(&mut get_remove_moves(
                &task,
                &slice,
                load_imbalance,
                &current_assignments,
                &slice_load,
            ));
        }
    }

    moves
}

// Returns task IDs of all tasks not equal to `task`.
fn other_tasks(task: &str, current_assignments: &HashMap<&str, Vec<Slice>>) -> Vec<String> {
    let mut tasks: Vec<String> = Vec::new();
    for &other_task in current_assignments.keys() {
        if other_task != task {
            tasks.push(String::from(other_task));
        }
    }
    tasks
}

fn get_reassign_moves(
    task: &str,
    slice: &Slice,
    current_load_imbalance: f32,
    current_assignments: &HashMap<&str, Vec<Slice>>,
    slice_load: &HashMap<Slice, i64>,
) -> Vec<Move> {
    let mut moves = Vec::new();

    let other_tasks = other_tasks(&task, current_assignments);
    for other_task in other_tasks {
        let mut assignments_copy = current_assignments.clone();
        apply_reassign_move(&task, &other_task, slice, &mut assignments_copy);

        let new_load_imbalance = load_imbalance(&assignments_copy, &slice_load);
        let weight = new_load_imbalance - current_load_imbalance;
        // TODO: Add key churn (jjohnson)
        let m = Move {
            move_type: MoveType::Reassign,
            source: String::from(task),
            destination: String::from(other_task),
            slice: *slice,
            weight: (weight.abs() * 1000.0) as i32,
            key_churn: 0,
        };
        moves.push(m);
    }

    moves
}

fn get_duplicate_moves(
    task: &str,
    slice: &Slice,
    current_load_imbalance: f32,
    current_assignments: &HashMap<&str, Vec<Slice>>,
    slice_load: &HashMap<Slice, i64>,
) -> Vec<Move> {
    let mut moves = Vec::new();

    let other_tasks = other_tasks(&task, current_assignments);
    for other_task in other_tasks {
        let mut assignments_copy = current_assignments.clone();
        apply_duplicate_move(&other_task, slice, &mut assignments_copy);

        let new_load_imbalance = load_imbalance(&assignments_copy, &slice_load);
        let weight = new_load_imbalance - current_load_imbalance;
        debug!(
            "task={}, other task={}, load imbalance={}, new load imbalance={}, weight={}",
            task, other_task, current_load_imbalance, new_load_imbalance, weight
        );
        // TODO: Add key churn (jjohnson)
        let m = Move {
            move_type: MoveType::Duplicate,
            source: String::from(task),
            destination: String::from(other_task),
            slice: *slice,
            weight: (weight.abs() * 1000.0) as i32,
            key_churn: 0,
        };
        moves.push(m);
    }

    moves
}

fn get_remove_moves(
    task: &str,
    slice: &Slice,
    current_load_imbalance: f32,
    current_assignments: &HashMap<&str, Vec<Slice>>,
    slice_load: &HashMap<Slice, i64>,
) -> Vec<Move> {
    let mut moves = Vec::new();

    let mut exists_elsewhere = false;
    let other_tasks = other_tasks(&task, current_assignments);
    for other_task in other_tasks {
        if current_assignments[other_task.as_str()].contains(&slice) {
            exists_elsewhere = true;
            break;
        }
    }

    if !exists_elsewhere {
        return moves;
    }

    let mut assignments_copy = current_assignments.clone();
    apply_remove_move(&task, slice, &mut assignments_copy);

    let new_load_imbalance = load_imbalance(&assignments_copy, &slice_load);
    let weight = new_load_imbalance - current_load_imbalance;
    // TODO: Add key churn (jjohnson)
    let m = Move {
        move_type: MoveType::Remove,
        source: String::from(task),
        destination: String::new(),
        slice: *slice,
        weight: (weight.abs() * 1000.0) as i32,
        key_churn: 0,
    };
    moves.push(m);

    moves
}

fn apply_reassign_move(
    source_task: &str,
    destination_task: &str,
    slice: &Slice,
    assignments: &mut HashMap<&str, Vec<Slice>>
) {
    // Remove slice from current task.
    let index = assignments[source_task]
        .iter()
        .position(|x| *x == *slice)
        .unwrap();
    assignments.get_mut(source_task).unwrap().remove(index);

    // Add slice to new task.
    assignments
        .get_mut(destination_task)
        .unwrap()
        .push(*slice);
}

fn apply_duplicate_move(
    destination_task: &str,
    slice: &Slice,
    assignments: &mut HashMap<&str, Vec<Slice>>
) {
    // Copy slice to new task.
    assignments
        .get_mut(destination_task)
        .unwrap()
        .push(*slice);
}

fn apply_remove_move(
    task: &str,
    slice: &Slice,
    assignments: &mut HashMap<&str, Vec<Slice>>
) {
    // Remove slice from current task.
    let index = assignments[task]
        .iter()
        .position(|x| *x == *slice)
        .unwrap();
    assignments.get_mut(task).unwrap().remove(index);
}

fn apply_move(
    mut assignments: &mut HashMap<&str, Vec<Slice>>,
    m: &Move
) {
    match &m.move_type {
        MoveType::Reassign => {
            apply_reassign_move(&m.source, &m.destination, &m.slice, &mut assignments);

            let source_update = Update {
                assigned: vec!(),
                unassigned: vec!(m.slice)
            };
            utils::send_update(&m.source, source_update).expect(format!("failed to send reassign update to task {}", m.source).as_str());

            let destination_update = Update {
                assigned: vec!(m.slice),
                unassigned: vec!()
            };
            utils::send_update(&m.destination, destination_update).expect(format!("failed to send reassign update to task {}", m.destination).as_str());
        },
        MoveType::Duplicate => {
            apply_duplicate_move(&m.destination, &m.slice, &mut assignments);

            let update = Update {
                assigned: vec!(m.slice),
                unassigned: vec!()
            };
            utils::send_update(&m.destination, update).expect(format!("failed to send duplicate update to task {}", m.destination).as_str());
        }
        MoveType::Remove => {
            apply_remove_move(&m.source, &m.slice, &mut assignments);

            let update = Update {
                assigned: vec!(),
                unassigned: vec!(m.slice)
            };
            utils::send_update(&m.source, update).expect(format!("failed to send remove update to task {}", m.source).as_str());
        }
    };
}
