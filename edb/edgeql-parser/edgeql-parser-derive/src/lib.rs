use proc_macro::TokenStream;

use syn::{parse_macro_input, Attribute, Type, TypePath};

use quote::quote;
use syn::{self, Fields, Ident};

#[proc_macro_derive(IntoPython, attributes(py_child, py_enum, py_union))]
pub fn into_python(input: TokenStream) -> TokenStream {
    use syn::Item;
    let mut item = parse_macro_input!(input as Item);
    match &mut item {
        Item::Enum(enum_) => impl_enum_into_python(enum_),
        Item::Struct(struct_) => impl_struct_into_python(struct_),
        unsupported => {
            syn::Error::new_spanned(unsupported, "IntoPython only supports structs and enums")
                .into_compile_error()
                .into()
        }
    }
}

fn impl_enum_into_python(enum_: &mut syn::ItemEnum) -> TokenStream {
    let variants = infer_variants(enum_);

    let name = &enum_.ident;
    let mut cases = Vec::new();

    if let Some(py_enum) = find_attr(&enum_.attrs, "py_enum") {
        let class_path = py_enum.meta.path();

        for Variant { name } in variants {
            cases.push(quote! {
                Self::#name => py.eval(#class_path.#name, None, None),
            });
        }
    } else if find_attr(&enum_.attrs, "py_child").is_some() {
        for Variant { name } in variants {
            cases.push(quote! {
                Self::#name(value) => value.into_python(py, parent),
            });
        }
    } else if find_attr(&enum_.attrs, "py_union").is_some() {
        for Variant { name } in variants {
            cases.push(quote! {
                Self::#name(value) => value.into_python(py, None),
            });
        }
    } else {
        panic!("enum is missing one of #[py_enum], #[py_child] or #[py_union]")
    }

    quote! {
        impl crate::into_python::IntoPython for #name {
            fn into_python(
                self,
                py: cpython::Python,
                parent: Option<cpython::PyDict>,
            ) -> cpython::PyResult<cpython::PyObject> {
                use crate::into_python::IntoPython;

                match self { #(#cases)* }
            }
        }
    }
    .into()
}

fn infer_variants(enum_: &syn::ItemEnum) -> Vec<Variant> {
    let mut variants = Vec::new();

    for variant in &enum_.variants {
        let name = variant.ident.clone();

        match &variant.fields {
            Fields::Named(_) => panic!("IntoPython does not support named enum variant fields"),
            Fields::Unnamed(fields) => {
                if fields.unnamed.len() != 1 {
                    panic!("IntoPython supports only enum variant fields with zero or one fields")
                }

                variants.push(Variant { name });
            }
            Fields::Unit => {
                variants.push(Variant { name });
            }
        }
    }

    variants
}

/// Information about the struct annotated with IntoPython
struct Variant {
    name: Ident,
}

fn impl_struct_into_python(struct_: &mut syn::ItemStruct) -> TokenStream {
    let (properties, py_child_field) = infer_fields(struct_);

    let name = &struct_.ident;

    let mut property_assigns = Vec::new();
    for property in properties {
        property_assigns.push(quote! {
            kw_args.set_item(
                py,
                stringify!(#property),
                self.#property.into_python(py, None)?
            )?;
        });
    }

    let init = if let Some(py_child_field) = py_child_field {
        let field = py_child_field.ident;
        if py_child_field.is_option {
            quote! {
                match self.#field {
                    Some(kind) => kind.into_python(py, Some(kw_args)),
                    None => crate::into_python::init_ast_class(py, stringify!(#name), kw_args)
                }
            }
        } else {
            quote! {
                self.#field.into_python(py, Some(kw_args))
            }
        }
    } else {
        quote! {
            crate::into_python::init_ast_class(py, stringify!(#name), kw_args)
        }
    };

    quote! {
        impl crate::into_python::IntoPython for #name {
            fn into_python(
                self,
                py: cpython::Python,
                parent_kw_args: Option<cpython::PyDict>,
            ) -> cpython::PyResult<cpython::PyObject> {
                use crate::into_python::IntoPython;

                let kw_args = parent_kw_args.unwrap_or_else(|| cPython::PyDict::new_bound(py));
                #(#property_assigns)*

                #init
            }
        }
    }
    .into()
}

struct PyChildField {
    ident: Ident,
    is_option: bool,
}

fn infer_fields(r#struct: &mut syn::ItemStruct) -> (Vec<Ident>, Option<PyChildField>) {
    let mut properties = Vec::new();
    let mut py_child = None;

    for field in &mut r#struct.fields {
        let ident = field
            .ident
            .clone()
            .expect("py_inherit supports only named fields");

        if find_attr(&field.attrs, "py_child").is_some() {
            let is_option = is_option(&field.ty);

            py_child = Some(PyChildField { ident, is_option });
            continue;
        }

        properties.push(ident);
    }

    (properties, py_child)
}

fn find_attr<'a>(attrs: &'a [Attribute], name: &'static str) -> Option<&'a Attribute> {
    attrs.iter().find(|a| {
        let Some(ident) = a.path().get_ident() else {
            return false;
        };
        *ident == name
    })
}

fn is_option(ty: &Type) -> bool {
    let Type::Path(TypePath { path, .. }) = ty else {
        return false;
    };
    let Some(segment) = path.segments.first() else {
        return false;
    };
    segment.ident == "Option"
}
