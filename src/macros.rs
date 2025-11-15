macro_rules! define_stats {
    ( $( $TypeName:ident => $field:ident : $ty:tt ),* $(,)? ) => {
        use std::sync::atomic::{AtomicU8};
        use $crate::channel::{Sender, Receiver};

        pub(crate) enum Action<T> {
            Increment,
            Decrement,
            Store(T),
        }

        // ---------- Atomic type mapping ----------
        macro_rules! AtomicFor {
            (bool)    => { AtomicBool };
            (u8)      => { AtomicU8 };
            (i8)      => { AtomicI8 };
            (u16)     => { AtomicU16 };
            (i16)     => { AtomicI16 };
            (u32)     => { AtomicU32 };
            (i32)     => { AtomicI32 };
            (u64)     => { AtomicU64 };
            (i64)     => { AtomicI64 };
            (u128)    => { AtomicU128 };
            (i128)    => { AtomicI128 };
            (usize)   => { AtomicUsize };
            (isize)   => { AtomicIsize };
        }

        // ---------- Inner struct ----------
        pub(crate) struct Inner {
            $( pub(crate) $field: AtomicFor!($ty), )*
        }

        impl Inner {
            pub(crate) fn new() -> Self {
                Self {
                    $( $field: <AtomicFor!($ty)>::new(0), )*
                }
            }

            // ---------- handle messages ----------
            pub(crate) fn handle(&self, msg: State) {
                match msg {
                    $(
                        State::$TypeName(m) => match m {
                            StateMsg::Get(reply) => {
                                let v = self.$field.load(Ordering::SeqCst);
                                let _ = reply.send(v);
                            },
                        StateMsg::Set(action) => match action {
                                Action::Increment => {
                                    self.$field.fetch_add(1, Ordering::SeqCst);
                                },
                                Action::Decrement => {
                                    self.$field.fetch_sub(1, Ordering::SeqCst);
                                },
                                Action::Store(val) => {
                                    self.$field.store(val, Ordering::SeqCst);
                                },
                            },
                        },
                    )*
                }
            }
        }

        // ---------- State enums ----------
        pub(crate) enum State {
            $( $TypeName(StateMsg<$ty>), )*
        }

        pub(crate) enum StateMsg<T> {
            Get(Sender<T>),
            Set(Action<T>),
        }
    };
}

// ---------- helper macros ----------
macro_rules! get_state {
    ($TypeName:ident, $tx:expr) => {{
        let chan = $crate::channel::unbounded();
        $tx.send($crate::State::$TypeName($crate::StateMsg::Get(
            chan.clone_sender(),
        )))
        .unwrap();
        chan.recv().unwrap()
    }};
}

macro_rules! set_state {
    ($TypeName:ident, $action:expr, $tx:expr) => {{
        $tx.send($crate::State::$TypeName($crate::StateMsg::Set($action)))
            .unwrap();
    }};
}
