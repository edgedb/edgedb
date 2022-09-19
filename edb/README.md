## Directory overview

Here is a list of most of the important directories in EdgeDB, along
with some of the key files and subdirectories in them.

This list is *partial*, focused on the compiler, and ordered conceptually.

 - `schema/` - Representation of the schema, and implementation of schema modifications (both SDL migrations and DDL statements).
   The schema is an immutable object (implemented as a bunch of immutable maps), and making changes to it produces a new schema object.
   Objects stored in the schema are represented in the compiler as proxy objects, and fetching attributes from them requires passing in a schema.

 - `edgeql/` - EdgeQL frontend tools - AST, parser, first stage compiler, etc
   - `edgeql/ast.py` - EdgeQL AST
   - `edgeql/parser/` - Parser. Uses https://github.com/MagicStack/parsing
   - `edgeql/compiler/` - Compiler from EdgeQL to our IR
   - `edgeql/tracer.py` and `edgeql/declarative.py` - Analysis to convert SDL schema descriptions to DDL that will create them.

 - `ir/` - Intermediate Representation (IR) and tools for operating on it
   - `edgeql/ir/pathid.py` - Definition of "path ids", which are used to identify sets
   - `edgeql/ir/ast.py` - Primary AST of intermediate representation.
   The IR contains no direct references to schema objects; information from
   the schema that is needed in the IR needs to be explicitly placed there.
   There are `TypeRef` and `PointerRef` objects that do this for types and pointers.
   - `edgeql/ir/scopetree.py` - Representation of "scope tree", which computes and tracks where sets are "bound". The IR output of the compiler consists of both an IR AST and a scope tree, needed to interpret it.

 - `pgsql/` - PostgreSQL backend tools - AST, codegen, second stage compiler, etc
   - `pgsql/ast.py` - SQL AST. The AST contains both information for the actual SQL AST, along with a large collection of metadata that is used during the compilation process.
   - `pgsql/codegen.py` - SQL codegen. Converts AST to SQL.
   - `pgsql/compiler/` - IR to SQL compiler.
   - `pgsql/delta.py` - Generates SQL DDL from delta trees.

 - `lib/` - Definition of EdgeDB's standard library
   - `lib/schema.edgeql` - Definition of the parts of the schema that are exposed publically.

 - `graphql/` - GraphQL to EdgeQL compiler

 - `server/` - Implementation of the EdgeDB server and protocol handling
   - `server/compiler` - The interface between the server and the compiler
