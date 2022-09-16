This is a checklist of steps needed to add a new type to EdgeDB, along with links to examples of PRs doing the tasks.

Core database range PRs:
 * https://github.com/edgedb/edgedb/pull/3983
 * https://github.com/edgedb/edgedb/pull/4020

Core database cal::duration PRs:
 * https://github.com/edgedb/edgedb/pull/3948

- [ ] JSON handling
  - [ ] Implement JSON casts if the default Postgres behavior won't work
  - [ ] Update output.serialize_expr_to_json(), if the default won't work
    * range: https://github.com/edgedb/edgedb/pull/4008

- [ ] If any new functions or constructors have an implementation that is not
      just purely a call to a strict function, make sure to test with inputs
      that are NULL at runtime!
      Probably the easiest way to generate NULL-at-runtime values is
      `<optional TYPE>$0` and then passing in `{}`.
  * range test example and bugfix: https://github.com/edgedb/edgedb/pull/4207/

- [ ] For compound types, add a schema class in edb/schema/types.py and

- [ ] Add mapping to pgsql types in edb/pgsql/types.py

- [ ] Add implementations of any relevant functions/operations to `edb/lib`.

- [ ] For compound types, add a type descriptor and code for encoding it in
edb/server/compiler/sertypes.py.
  * range: https://github.com/edgedb/edgedb/pull/4016

- [ ] For new scalar types, add it to edb/api/types.txt and edb/graphql/types.py. Run `edb gen-types`.

- [ ] Update all of the first-party language drivers (or get their owners to)
  - [ ] Python (Fantix/Elvis/Sully)
    * cal::date_duration: https://github.com/edgedb/edgedb-python/pull/335
    * range: https://github.com/edgedb/edgedb-python/pull/332/
  - [ ] Go (Frederick)
    * cal::date_duration: https://github.com/edgedb/edgedb-go/pull/232
  - [ ] Javascript (James/Colin)
    * cal::date_duration: https://github.com/edgedb/edgedb-js/pull/373/
    * range: https://github.com/edgedb/edgedb-js/pull/377
  - [ ] Rust/CLI (Paul)
    * This requires updating both the Rust bindings to support the
      new type and the CLI to properly print it
    * cal::date_duration: https://github.com/edgedb/edgedb-rust/pull/146, https://github.com/edgedb/edgedb-cli/pull/759
    * range: https://github.com/edgedb/edgedb-rust/pull/145, https://github.com/edgedb/edgedb-cli/pull/755

- [ ] Add a field of the new type to the `dump` test for the new version

- [ ] Write tests.
