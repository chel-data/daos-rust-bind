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

#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]

include!(concat!(env!("OUT_DIR"), "/daos-bindings.rs"));

pub const DAOS_TXN_NONE: daos_handle_t = daos_handle_t {cookie: 0u64};

pub fn daos_anchor_is_eof(anchor: &daos_anchor_t) -> bool
{
    daos_anchor_type_t_DAOS_ANCHOR_TYPE_EOF == anchor.da_type.into()
}


#[cfg(test)]
mod tests {
    use super::*;
    use std::ptr;
    use std::ffi::CString;

    #[test]
    fn test_daos_binding_basic() -> () {
        unsafe {
            let res = daos_init();
            assert_eq!(res, 0);

            let mut poh = daos_handle_t{ cookie: 0u64 };
            let pool_name = CString::new("pool1").expect("Allocate CString failed");
            
            let res = daos_pool_connect2(pool_name.as_ptr(),
                                         ptr::null_mut(),
                                         DAOS_PC_RW,
                                         &mut poh,
                                         ptr::null_mut(),
                                         ptr::null_mut());
            assert_eq!(res, 0);

            daos_pool_disconnect(poh, ptr::null_mut());
            daos_fini();
        }
    }
}

