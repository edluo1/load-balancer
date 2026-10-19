use std::thread::spawn;

pub fn test_threads() {
    let mut x = 0u128;
    let y = 1u128;
    for i in 1..500_080_000 {
        x += i + y;
    }
    println!("{x}");
}

pub fn spawn_thread() {
    let thread_fn = || {
        test_threads();
    };
    println!("Started");
    let handle = spawn(thread_fn);
    let handle2 = spawn(thread_fn);
    println!("Thread completed...");

    handle.join();
    handle2.join();
    println!("Task completed...");
}