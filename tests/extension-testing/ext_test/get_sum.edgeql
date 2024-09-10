create extension package ext_test VERSION '0.1' {
  set ext_module := "ext::ext_test";
  set sql_extensions := ["get_sum"];

  create module ext::ext_test;
  create function ext::ext_test::get_sum(x: std::int32, y: std::int32) -> std::int32 {
    USING SQL $$
      select get_sum(x, y)
    $$
  };
};
