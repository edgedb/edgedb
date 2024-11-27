use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::parse_macro_input;

#[proc_macro_derive(Reduce, attributes(output, stub))]
pub fn grammar_non_terminal(input: TokenStream) -> TokenStream {
    let item = parse_macro_input!(input as syn::Item);

    let syn::Item::Enum(enum_item) = item else {
        panic!("Only enums are allowed to be grammar rules")
    };

    let name = &enum_item.ident;
    let data_name = format_ident!("{name}Data");

    let output_ty = find_list_attribute(&enum_item, "output")
        .unwrap_or_else(|| panic!("missing #[output(...)] attribute"));
    let output_ty: TokenStream = output_ty.clone().into();
    let output_ty: syn::Type = parse_macro_input!(output_ty as syn::Type);

    let is_stub = find_list_attribute(&enum_item, "stub").is_some();

    let mut data_variants = proc_macro2::TokenStream::new();
    for variant in &enum_item.variants {
        let variant_name = &variant.ident;

        let mut kids = proc_macro2::TokenStream::new();
        for non_term_name in iter_non_terminals(&variant_name.to_string()) {
            let non_term_name = format_ident!("{non_term_name}");
            kids.extend(quote! {
                <#non_term_name as Reduce>::Output,
            });
        }

        data_variants.extend(quote! {
            #variant_name(#kids),
        });
    }

    let mut match_arms = proc_macro2::TokenStream::new();
    for variant in &enum_item.variants {
        let variant_name = &variant.ident;

        let mut args = proc_macro2::TokenStream::new();
        let mut calls = proc_macro2::TokenStream::new();
        for (index, non_term_name) in iter_non_terminals(&variant_name.to_string()).enumerate() {
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

                let data = #data_name::#variant_name(#args);
                #output_ty::from(data)
            }
        });
    }

    let mut stub = proc_macro2::TokenStream::new();
    if is_stub {
        stub.extend(quote! {
            impl From<#data_name> for #output_ty {
                fn from(val: #data_name) -> Self {
                    todo!();
                }
            }
        })
    }

    let output = quote!(
        enum #data_name {
            #data_variants
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
    );

    TokenStream::from(output)
}

fn find_list_attribute<'a>(
    enum_item: &'a syn::ItemEnum,
    name: &'static str,
) -> Option<&'a proc_macro2::TokenStream> {
    enum_item.attrs.iter().find_map(|x| match &x.meta {
        syn::Meta::List(ml) => {
            if path_eq(&ml.path, name) {
                Some(&ml.tokens)
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

fn iter_non_terminals(variant_name: &str) -> impl Iterator<Item = &str> {
    variant_name
        .split('_')
        .filter(|c| *c != "epsilon" && *c != c.to_ascii_uppercase())
}
