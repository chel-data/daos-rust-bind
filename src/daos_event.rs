/*
 *  Copyright (C) 2024 github.com/chel-data
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

use crate::bindings::{
    daos_eq_create, daos_eq_destroy, daos_eq_poll, daos_event__bindgen_ty_1, daos_event_fini,
    daos_event_init, daos_event_register_comp_cb, daos_event_t, daos_event_test, daos_handle_t,
    DAOS_EQ_NOWAIT,
};
use std::io::{Error, ErrorKind, Result};
use std::ptr;
use std::sync::mpsc;
use std::thread;
use tokio::sync::oneshot;

#[derive(Debug)]
pub struct CallbackArg {
    magic: u32,
    tx: Option<oneshot::Sender<i32>>,
}

#[derive(Debug)]
pub struct DaosEvent {
    event: Option<Box<daos_event_t>>,
}

unsafe extern "C" fn event_callback(
    arg1: *mut ::std::os::raw::c_void,
    _arg2: *mut daos_event_t,
    arg3: ::std::os::raw::c_int,
) -> i32 {
    let raw_arg = arg1 as *mut CallbackArg;
    let call_arg = Box::from_raw(raw_arg);
    println!("event_callback is called, magic={:#x}", call_arg.magic);
    match call_arg.tx {
        Some(tx) => {
            if let Err(_) = tx.send(arg3) {
                eprintln!("Failed to send event callback result");
                -1
            } else {
                0
            }
        }
        None => -1,
    }
}

impl DaosEvent {
    pub fn new(eqh: daos_handle_t) -> Result<Self> {
        let mut event = Box::new(daos_event_t {
            ev_error: 0,
            ev_private: daos_event__bindgen_ty_1 { space: [0u64; 20] },
            ev_debug: 0u64,
        });

        let ret = unsafe { daos_event_init(event.as_mut(), eqh, ptr::null_mut()) };
        if ret != 0 {
            return Err(Error::new(ErrorKind::Other, "can't init daos event"));
        }

        Ok(DaosEvent { event: Some(event) })
    }

    pub fn as_mut(&mut self) -> &mut daos_event_t {
        self.event.as_mut().unwrap().as_mut()
    }

    pub fn register_callback(&mut self) -> Result<oneshot::Receiver<i32>> {
        let (tx, rx) = oneshot::channel::<i32>();
        let call_arg = Box::new(CallbackArg {
            magic: 0x1caffe1d,
            tx: Some(tx),
        });

        let ret = unsafe {
            daos_event_register_comp_cb(
                self.as_mut(),
                Some(event_callback),
                Box::into_raw(call_arg) as *mut ::std::os::raw::c_void,
            )
        };
        if ret != 0 {
            return Err(Error::new(
                ErrorKind::Other,
                "can't register event callback",
            ));
        }

        Ok(rx)
    }
}

impl Drop for DaosEvent {
    fn drop(&mut self) {
        match self.event {
            Some(ref mut event) => {
                let mut status: bool = false;
                let ret =
                    unsafe { daos_event_test(event.as_mut(), DAOS_EQ_NOWAIT.into(), &mut status) };
                // if status is false, event is still in queue
                if ret == 0 {
                    if status {
                        let ret = unsafe { daos_event_fini(event.as_mut()) };
                        if ret != 0 {
                            eprintln!("Failed to fini daos event");
                        } else {
                            self.event.take();
                        }
                    } else {
                        eprintln!("event is still in queue");
                    }
                } else {
                    eprintln!("fail to test event status");
                }
            }
            None => {}
        }
    }
}

#[derive(Debug)]
pub struct DaosEventQueue {
    handle: Option<daos_handle_t>,
    sender: mpsc::Sender<i32>,
    thread_handle: Option<thread::JoinHandle<()>>,
}

impl DaosEventQueue {
    pub fn new() -> Result<DaosEventQueue> {
        let mut eqh: daos_handle_t = daos_handle_t { cookie: 0u64 };
        let res = unsafe { daos_eq_create(&mut eqh) };

        let (snd, rcv) = mpsc::channel::<i32>();

        let t_handle = thread::spawn(move || {
            let n_events = 10u32;
            let mut events = std::vec::Vec::with_capacity(n_events as usize);
            events.resize(10, ptr::null_mut::<daos_event_t>());

            while rcv.try_recv().is_err() {
                let ret = unsafe { daos_eq_poll(eqh, 1, 50, n_events, events.as_mut_ptr()) };
                if ret < 0 {
                    eprintln!("pool event queue failed, ret={}", ret);
                }
            }
        });

        if res == 0 {
            Ok(DaosEventQueue {
                handle: Some(eqh),
                sender: snd,
                thread_handle: Some(t_handle),
            })
        } else {
            Err(Error::new(ErrorKind::Other, "can't create event queue"))
        }
    }

    pub fn get_handle(&self) -> Option<daos_handle_t> {
        self.handle.clone()
    }

    pub fn create_event(&self) -> Result<DaosEvent> {
        DaosEvent::new(self.handle.unwrap())
    }
}

impl Drop for DaosEventQueue {
    fn drop(&mut self) {
        if let Some(eqh) = self.handle {
            match self.sender.send(0) {
                Ok(_) => {
                    let join_handle = self.thread_handle.take();
                    let _ = join_handle.unwrap().join();
                }
                Err(_) => return,
            };

            let res = unsafe { daos_eq_destroy(eqh, 0) };
            if res != 0 {
                eprintln!("Failed to destroy event queue");
            } else {
                self.handle.take();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::daos_pool::DaosPool;

    #[tokio::test]
    async fn test_create_async_event1() {
        let _pool = DaosPool::new("pool1");

        let eqh = DaosEventQueue::new().unwrap();
        let mut evt = eqh.create_event().unwrap();
        assert!(evt.event.is_some());

        let _rx = evt.register_callback().unwrap();
    }

    #[test]
    fn test_destroy_event_queue_success() {
        let _pool = DaosPool::new("pool1");

        let eqh = DaosEventQueue::new().unwrap();
        drop(eqh);
    }
}
