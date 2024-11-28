use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::parse_macro_input;

const OUTPUT_ATTR: &str = "output";
const STUB_ATTR: &str = "stub";
const LIST_ATTR: &str = "list";
const LIST_SEPARATOR_PATH: &str = "separator";
const LIST_TRAILING_PATH: &str = "trailing";

/// Implements [edgeql_parser::grammar::Reduce] for conversion from CST nodes to
/// AST nodes.
///
/// Requires an enum with unit variants only. Each variant name is interpreted
/// as a "production name", which consists of names of parser terms (either
/// Terminals or NonTerminals), delimited by `_`.
///
/// Requires an `#[output(...)]` attribute, which denotes the output type of
/// this parser non-terminal.
///
/// Will generate a `*Node` enum, which contains the reduced AST nodes of child
/// non-terminals. This "node" requires an implementation of `Into<OutputTy>`,
/// or rather `OutputTy` requires `From<Node>`.
///
/// If `#[stub()]` attribute is present, the `From` trait is automatically
/// derived, filled with `todo!()`.
///
/// If `#[list(separator=..., trailing=...)]` attribute is present,
/// a `*List` enum is automatically generated with the
/// [edgeql_parser::grammar::Reduce] implementation for that enum,
/// where the `separator` path value will be used to separate list items.
/// If the optional `trailing` path is set to `true`, a `*ListInner` enum
/// is also generated to allow the use of trailing separators.
#[proc_macro_derive(Reduce, attributes(output, stub, list))]
pub fn grammar_non_terminal(input: TokenStream) -> TokenStream {
    let item = parse_macro_input!(input as syn::Item);

    let syn::Item::Enum(enum_item) = item else {
        panic!("Only enums are allowed to be grammar rules")
    };

    let name = &enum_item.ident;
    let node_name = format_ident!("{name}Node");

    let output_ty = get_list_attribute_tokens(&enum_item, OUTPUT_ATTR)
        .unwrap_or_else(|| panic!("missing #[output(...)] attribute"));
    let output_ty: TokenStream = output_ty.clone().into();
    let output_ty: syn::Type = parse_macro_input!(output_ty as syn::Type);

    let is_stub = get_list_attribute(&enum_item, STUB_ATTR).is_some();

    let mut node_variants = proc_macro2::TokenStream::new();
    for variant in &enum_item.variants {
        let variant_name = &variant.ident;

        let mut kids = proc_macro2::TokenStream::new();
        for (_index, non_term_name) in iter_non_terminals(&variant_name.to_string()) {
            let non_term_name = format_ident!("{non_term_name}");
            kids.extend(quote! {
                <#non_term_name as Reduce>::Output,
            });
        }

        node_variants.extend(quote! {
            #variant_name(#kids),
        });
    }

    let mut match_arms = proc_macro2::TokenStream::new();
    for variant in &enum_item.variants {
        let variant_name = &variant.ident;

        let mut args = proc_macro2::TokenStream::new();
        let mut calls = proc_macro2::TokenStream::new();
        for (index, non_term_name) in iter_non_terminals(&variant_name.to_string()) {
            let arg_name = format_ident!("arg{index}");
            let non_term_name = format_ident!("{non_term_name}");

            calls.extend(quote! {
                let #arg_name = <#non_term_name as Reduce>::reduce(&p.args[#index]);
            });

            args.extend(quote! { #arg_name, });
        }

        match_arms.extend(quote! {
            Self::#variant_name => {
                #calls

                let node = #node_name::#variant_name(#args);
                <#node_name as Into<#output_ty>>::into(node)
            }
        });
    }

    let mut stub = proc_macro2::TokenStream::new();
    if is_stub {
        stub.extend(quote! {
            impl From<#node_name> for #output_ty {
                fn from(val: #node_name) -> Self {
                    todo!();
                }
            }
        })
    }

    let mut list = proc_macro2::TokenStream::new();
    if let Some(list_attr) = get_list_attribute(&enum_item, LIST_ATTR) {
        generate_list(&mut list, name, &output_ty, list_attr);
    }

    let output = quote!(
        enum #node_name {
            #node_variants
        }

        impl Reduce for #name {
            type Output = #output_ty;

            fn reduce(node: &CSTNode) -> #output_ty {
                let CSTNode::Production(p) = node else { panic!() };
                match Self::from_id(p.id) {
                    #match_arms
                }
            }
        }

        #stub

        #list
    );

    TokenStream::from(output)
}

fn get_list_attribute_tokens<'a>(
    enum_item: &'a syn::ItemEnum,
    name: &'static str,
) -> Option<&'a proc_macro2::TokenStream> {
    get_list_attribute(enum_item, name).and_then(|a| match &a.meta {
        syn::Meta::List(ml) => Some(&ml.tokens),
        _ => None,
    })
}

fn get_list_attribute<'a>(
    enum_item: &'a syn::ItemEnum,
    name: &'static str,
) -> Option<&'a syn::Attribute> {
    enum_item.attrs.iter().find_map(|a| match &a.meta {
        syn::Meta::List(ml) => {
            if path_eq(&ml.path, name) {
                Some(a)
            } else {
                None
            }
        }
        _ => None,
    })
}

fn path_eq(path: &syn::Path, name: &str) -> bool {
    path.get_ident().map_or(false, |i| i.to_string() == name)
}

fn iter_non_terminals(variant_name: &str) -> impl Iterator<Item = (usize, &str)> {
    variant_name
        .split('_')
        .enumerate()
        .filter(|c| c.1 != "epsilon" && c.1 != c.1.to_ascii_uppercase())
}

fn generate_list(
    list_stream: &mut proc_macro2::TokenStream,
    element: &syn::Ident,
    output_ty: &syn::Type,
    attr: &syn::Attribute,
) {
    let mut separator = None;
    let mut allow_trailing_list = false;

    attr.parse_nested_meta(|m| {
        if path_eq(&m.path, LIST_SEPARATOR_PATH) {
            separator = m.value()?.parse::<Option<syn::Ident>>()?;
        } else if path_eq(&m.path, LIST_TRAILING_PATH) {
            allow_trailing_list = m.value()?.parse::<syn::LitBool>()?.value;
        }

        Ok(())
    })
    .unwrap_or_else(|_| panic!("Internal error during parsing of #[list()] attribute"));

    if separator.is_none() {
        panic!("`separator` path for `#[list(...)]` attribute is required")
    }

    let separator = separator.unwrap();

    if allow_trailing_list {
        let mut list_inner_stream = proc_macro2::TokenStream::new();

        let list_inner = format_ident!("{element}ListInner");
        let list_inner_node = format_ident!("{element}ListInnerNode");
        let inner_sep_elem = format_ident!("{list_inner}_{separator}_{element}");

        list_inner_stream.extend(quote! {
            #[derive(edgeql_parser_derive::Reduce)]
            #[output(Vec::<#output_ty>)]
            pub enum #list_inner {
                #element,
                #inner_sep_elem,
            }

            impl From<#list_inner_node> for Vec<#output_ty> {
                fn from(val: #list_inner_node) -> Self {
                    match val {
                        #list_inner_node::#element(e) => {
                            vec![e]
                        },
                        #list_inner_node::#inner_sep_elem(mut l, e) => {
                            l.push(e);
                            l
                        },
                    }
                }
            }
        });

        let list = format_ident!("{element}List");
        let list_node = format_ident!("{element}ListNode");
        let list_inner_sep = format_ident!("{list_inner}_{separator}");

        list_stream.extend(quote! {
            #[derive(edgeql_parser_derive::Reduce)]
            #[output(Vec::<#output_ty>)]
            pub enum #list {
                #list_inner,
                #list_inner_sep,
            }

            impl From<#list_node> for Vec<#output_ty> {
                fn from(val: #list_node) -> Self {
                    match val {
                        #list_node::#list_inner(l) => l,
                        #list_node::#list_inner_sep(l) => l,
                    }
                }
            }

            #list_inner_stream
        });
    } else {
        let list = format_ident!("{element}List");
        let list_node = format_ident!("{element}ListNode");
        let list_sep_elem = format_ident!("{list}_{separator}_{element}");

        list_stream.extend(quote! {
            #[derive(edgeql_parser_derive::Reduce)]
            #[output(Vec::<#output_ty>)]
            pub enum #list {
                #element,
                #list_sep_elem,
            }

            impl From<#list_node> for Vec<#output_ty> {
                fn from(val: #list_node) -> Self {
                    match val {
                        #list_node::#element(e) => {
                            vec![e]
                        },
                        #list_node::#list_sep_elem(mut l, e) => {
                            l.push(e);
                            l
                        },
                    }
                }
            }
        });
    }
}
