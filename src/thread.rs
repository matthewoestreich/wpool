use std::sync::Arc;

use crate::channel::{Channel, unbounded};

//
// Pauser is a 'quality-of-life' wrapper to simplify thread sync between
// a controller thread and one (or many) non-controller (worker) threads.
//
// You still need to handle the 'paused' logic on your non-controller threads.
// This doesn't just magically pause a thread.
//
// You can check out `worker.rs` and `wpool.rs` to see how we use it.
//
// ~~ Example usage ~~
//
//  On non-controller thread(s):
//
//      ```rust
//      fn spawn_worker(pauser: Arc<Pauser>) -> thread::JoinHandle<()> {
//          thread::spawn(move || {
//              loop {
//                  your_worker_logic(); // <-- do whatever, etc..
//                  pauser.pause_this_thread(); // <-- Let them know we are paused, blocks until we are resumed.
//              }
//          })
//      }
//      ```
//
//  On controller thread: (this is pseudo code and will not actually compile)
//
//      ```rust
//      let pauser = Pauser::new();
//
//      /* Clone the pauser for every non-controller thread. */
//
//      let handle1 = spawn_worker(Arc::clone(&pauser)); // <-- `spawn_worker` fn from above
//      let handle2 = spawn_worker(Arc::clone(&pauser));
//
//      my_workers.push(handle1, handle2); // <-- all of your spawned threads that use a pauser.
//
//      for _ in my_workers { // <-- if you spawned N threads you can wait for all of them to pause.
//           pauser.recv_ack(); // <-- blocks
//      }
//
//      do_some_more_work(); // <-- do work, etc..
//
//      for _ in my_workers {
//           pauser.send_resume(); // <-- resume all workers/non-controller threads.
//      }
//      ```
//
pub(crate) struct Pauser {
    ack_chan: Channel<()>,
    pause_chan: Channel<()>,
}

impl Pauser {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            ack_chan: unbounded(),
            pause_chan: unbounded(),
        })
    }

    // Call from non-controller thread, like a worker thread.
    // Lets the controller thread know we are paused and then
    // blocks until controller thread sends the resume message.
    pub(crate) fn pause_this_thread(&self) {
        let _ = self.ack_chan.send(());
        let _ = self.pause_chan.recv();
    }

    // Call from controller thread.
    // Blocks until non-controller thread tells us they are paused.
    pub(crate) fn recv_ack(&self) {
        let _ = self.ack_chan.recv();
    }

    // Call from controller thread.
    // Sends resume message to non-controller thread.
    pub(crate) fn send_resume(&self) {
        let _ = self.pause_chan.send(());
    }
}
