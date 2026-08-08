//! The only adapter module permitted to reinterpret Arrow C ABI structs.

use std::mem::{align_of, size_of};

use arrow_array::ffi::{from_ffi, FFI_ArrowArray};
use arrow_data::ArrayData;
use arrow_schema::ffi::FFI_ArrowSchema;
use polars_arrow::array::Array as PolarsArray;
use polars_arrow::datatypes::{ArrowDataType as PolarsArrowDataType, Field as PolarsField};
use polars_arrow::ffi::{
    export_array_to_c, export_field_to_c, ArrowArray as PolarsArrowArray,
    ArrowSchema as PolarsArrowSchema,
};
use stillflow_core::{ConnectorError, ConnectorResult, ErrorCategory};

const _: () = assert!(size_of::<PolarsArrowArray>() == size_of::<FFI_ArrowArray>());
const _: () = assert!(align_of::<PolarsArrowArray>() == align_of::<FFI_ArrowArray>());
const _: () = assert!(size_of::<PolarsArrowSchema>() == size_of::<FFI_ArrowSchema>());
const _: () = assert!(align_of::<PolarsArrowSchema>() == align_of::<FFI_ArrowSchema>());

pub(super) fn import_array(
    field: &PolarsField,
    array: Box<dyn PolarsArray>,
) -> ConnectorResult<ArrayData> {
    let array = export_array_to_c(array);
    let schema = export_field_to_c(field);
    let normalize_null_buffers = matches!(field.dtype, PolarsArrowDataType::Null);
    // SAFETY: Both crates expose the Arrow C Data Interface structs with
    // `repr(C)`. The compile-time size/alignment assertions above guard the ABI
    // layout used by this release pair. Ownership moves exactly once into
    // arrow-rs, whose Drop implementation invokes Polars' exported release
    // callbacks. `from_raw` replaces each producer wrapper with an inert empty
    // C ABI value, preventing a second release when that wrapper is dropped.
    unsafe { import_exported(array, schema, normalize_null_buffers) }
}

unsafe fn import_exported(
    array: PolarsArrowArray,
    schema: PolarsArrowSchema,
    normalize_null_buffers: bool,
) -> ConnectorResult<ArrayData> {
    let mut array = array;
    let mut schema = schema;
    let mut arrow_array = unsafe {
        FFI_ArrowArray::from_raw((&mut array as *mut PolarsArrowArray).cast::<FFI_ArrowArray>())
    };
    let arrow_schema = unsafe {
        FFI_ArrowSchema::from_raw((&mut schema as *mut PolarsArrowSchema).cast::<FFI_ArrowSchema>())
    };
    // Polars 0.46 exports one placeholder buffer for the physical Null type,
    // while Arrow 59 enforces the C Data Interface's zero-buffer layout. The
    // producer release callback owns that placeholder through `private_data`
    // and does not consult these public view fields, so normalizing the consumer
    // view preserves ownership while making the payload spec-conforming.
    if normalize_null_buffers {
        arrow_array.n_buffers = 0;
        arrow_array.buffers = std::ptr::null_mut();
    }
    // SAFETY: The producer-created array and schema remain paired, and their
    // release callbacks and private data were moved into the arrow-rs owners.
    unsafe { from_ffi(arrow_array, &arrow_schema) }.map_err(|error| {
        ConnectorError::with_category(
            ErrorCategory::Internal,
            false,
            "the Arrow C Data Interface payload could not be imported",
            vec![error.to_string()],
            std::collections::BTreeMap::new(),
        )
    })
}
