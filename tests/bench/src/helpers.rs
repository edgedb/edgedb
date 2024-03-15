pub fn to_edgedb_enum<T>(enum_value: T) -> EValue
where
	T: std::fmt::Display,
{
	EValue::Enum(EnumValue::from(enum_value.to_string().as_str()))
}

use edgedb_protocol::{
	codec::{ObjectShape, ShapeElement},
	common::Cardinality,
	value::{EnumValue, Value as EValue},
};
// https://quan.hoabinh.vn/post/2023/8/querying-edgedb-with-name-parameters-in-rust
pub fn edge_object_from_pairs<N, V>(iter: impl IntoIterator<Item = (N, (V, Cardinality))>) -> EValue
where
	N: ToString,
	V: Into<Option<EValue>>,
{
	let mut elements = Vec::new();
	let mut fields: Vec<Option<EValue>> = Vec::new();
	for (key, (val, cardinality)) in iter.into_iter() {
		elements.push(create_shape_element(key, cardinality));
		fields.push(val.into());
	}
	EValue::Object {
		shape: ObjectShape::new(elements),
		fields,
	}
}

fn create_shape_element<N: ToString>(name: N, cardinality: Cardinality) -> ShapeElement {
	ShapeElement {
		name: name.to_string(),
		cardinality: Some(cardinality),
		flag_link: false,
		flag_link_property: false,
		flag_implicit: false,
	}
}

#[macro_export]
macro_rules! edgedb_args {
   ($($key:expr => ($value:expr, $cd:expr)),*) => {
      $crate::helpers::edge_object_from_pairs(indexmap::indexmap! {
         $($key => ($value, $cd)),*
      })
   };
}
