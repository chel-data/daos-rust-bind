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

use std::env;
use std::path::PathBuf;

fn main() {
    // Tell cargo to look for shared libraries in the specified directory
    println!("cargo:rustc-link-search=/usr/lib64");

    // Tell cargo to tell rustc to link the system bzip2
    // shared library.
    println!("cargo:rustc-link-lib=daos");
    println!("cargo:rustc-link-lib=daos_common");

    // The bindgen::Builder is the main entry point
    // to bindgen, and lets you build up options for
    // the resulting bindings.
    let bindings = bindgen::Builder::default()
        // The input header we would like to generate
        // bindings for.
        .header("/usr/include/daos.h")
        .allowlist_file("/usr/include/daos_api.h")
        .allowlist_file("/usr/include/daos_fs.h")
        .allowlist_file("/usr/include/daos_obj_class.h")
        .allowlist_file("/usr/include/daos_security.h")
        .allowlist_file("/usr/include/daos_array.h")
        .allowlist_file("/usr/include/daos_fs_sys.h")
        .allowlist_file("/usr/include/daos_obj.h")
        .allowlist_file("/usr/include/daos_task.h")
        .allowlist_file("/usr/include/daos_cont.h")
        .allowlist_file("/usr/include/daos.h")
        .allowlist_file("/usr/include/daos_pool.h")
        .allowlist_file("/usr/include/daos_types.h")
        .allowlist_file("/usr/include/daos_errno.h")
        .allowlist_file("/usr/include/daos_kv.h")
        .allowlist_file("/usr/include/daos_prop.h")
        .allowlist_file("/usr/include/daos_uns.h")
        .allowlist_file("/usr/include/daos_event.h")
        .allowlist_file("/usr/include/daos_mgmt.h")
        .allowlist_file("/usr/include/daos_s3.h")
        .allowlist_file("/usr/include/daos_version.h")
        .allowlist_file("/usr/include/gurt/types.h")
        // Tell cargo to invalidate the built crate whenever any of the
        // included header files changed.
        .parse_callbacks(Box::new(bindgen::CargoCallbacks::new()))
        // Finish the builder and generate the bindings.
        .generate()
        // Unwrap the Result and panic on failure.
        .expect("Unable to generate bindings");

    // Write the bindings to the $OUT_DIR/bindings.rs file.
    let out_path = PathBuf::from(env::var("OUT_DIR").unwrap());
    bindings
        .write_to_file(out_path.join("daos-bindings.rs"))
        .expect("Couldn't write bindings!");
}
