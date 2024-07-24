.. _ref_guide_rest_apis_with_fastapi:

=======
FastAPI
=======

:edb-alt-title: Building a REST API with EdgeDB and FastAPI

Because FastAPI encourages and facilitates strong typing, it's a natural
pairing with EdgeDB. Our Python code generation generates not only typed
query functions but result types you can use to annotate your endpoint handler
functions.

EdgeDB can help you quickly build REST APIs in Python without getting into the
rigmarole of using ORM libraries to handle your data effectively. Here, we'll
be using `FastAPI <https://fastapi.tiangolo.com/>`_ to expose the API endpoints
and EdgeDB to store the content.

We'll build a simple event management system where you'll be able to fetch,
create, update, and delete *events* and *event hosts* via RESTful API
endpoints.

Watch our video tour of this example project to get a preview of what you'll be
building in this guide:

.. edb:youtube-embed:: OZ_UURzDkow

Prerequisites
=============

Before we start, make sure you've :ref:`installed <ref_admin_install>` the
``edgedb`` command line tool. For this tutorial, we'll use Python 3.10 to 
take advantage of the asynchronous I/O paradigm to communicate with the 
database more efficiently. You can use newer versions of Python if you prefer, 
but you may need to adjust the code accordingly. If you want to skip ahead, 
the completed source code for this API can be found `in our examples repo
<https://github.com/edgedb/edgedb-examples/tree/main/fastapi-crud>`_. If you
want to check out an example with EdgeDB Auth, you can find that in the same
repo in the `fastapi-crud-auth folder
<https://github.com/edgedb/edgedb-examples/tree/main/fastapi-crud-auth>`_.


Create a project directory
^^^^^^^^^^^^^^^^^^^^^^^^^^

To get started, create a directory for your project and change into it.

.. code-block:: bash

    $ mkdir fastapi-crud
    $ cd fastapi-crud


Install the dependencies
^^^^^^^^^^^^^^^^^^^^^^^^

Create a Python virtual environment, activate it, and
install the dependencies with this command (in Linux/macOS; see the following
note for help with Windows):

.. code-block:: bash

    $ python -m venv myvenv
    $ source myvenv/bin/activate
    $ pip install edgedb fastapi 'httpx[cli]' uvicorn

.. note::

    Make sure you run ``source myvenv/bin/activate`` any time you want to come
    back to this project to activate its virtual environment. If not, you may
    start working under your system's default Python environment which could be
    the incorrect version or not have the dependencies installed. If you want
    to confirm you're using the right environment, run ``which python``. You
    should see that the current ``python`` is inside your venv directory.

.. note::

    The commands will differ for Windows/Powershell users; `this guide
    <https://realpython.com/python-virtual-environments-a-primer/>`_ provides
    instructions for working with virtual environments across a range of OSes,
    including Windows.

Initialize the database
^^^^^^^^^^^^^^^^^^^^^^^

Now, let's initialize an EdgeDB project. From the project's root directory:

.. code-block:: bash

    $ edgedb project init
    No `edgedb.toml` found in `<project-path>` or above
    Do you want to initialize a new project? [Y/n]
    > Y
    Specify the name of EdgeDB instance to use with this project [default:
    fastapi_crud]:
    > fastapi_crud
    Checking EdgeDB versions...
    Specify the version of EdgeDB to use with this project [default: 2.7]:
    > 2.7

Once you've answered the prompts, a new EdgeDB instance called ``fastapi_crud``
will be created and started. If you see ``Project initialized``, you're ready.


Connect to the database
^^^^^^^^^^^^^^^^^^^^^^^

Let's test that we can connect to the newly started instance. To do so, run:

.. code-block:: bash

    $ edgedb

You should see this prompt indicating you are now connected to your new
database instance:

::

    EdgeDB 2.x (repl 2.x)
    Type \help for help, \quit to quit.
    edgedb>

You can start writing queries here. Since this database is empty, that won't
get you very far, so let's start designing our data model instead.

Schema design
=============

The event management system will have two entities: **events** and **users**.
Each *event* can have an optional link to a *user* who is that event's host.
The goal is to create API endpoints that'll allow us to fetch, create, update,
and delete the entities while maintaining their relationships.

EdgeDB allows us to declaratively define the structure of the entities. If
you've worked with SQLAlchemy or Django ORM, you might refer to these
declarative schema definitions as *models*. In EdgeDB we call them
"object types".

The schema lives inside ``.esdl`` files in the ``dbschema`` directory. It's
common to declare the entire schema in a single file
``dbschema/default.esdl``. This file is created for you when you run ``edgedb
project init``, but you'll need to fill it with your schema. This is what our
datatypes look like:

.. code-block:: sdl
    :caption: dbschema/default.esdl

    module default {
      abstract type Auditable {
        required created_at: datetime {
          readonly := true;
          default := datetime_current();
        }
      }

      type User extending Auditable {
        required name: str {
          constraint exclusive;
          constraint max_len_value(50);
        };
      }

      type Event extending Auditable {
        required name: str {
          constraint exclusive;
          constraint max_len_value(50);
        }
        address: str;
        schedule: datetime;
        link host: User;
      }
    }

Here, we've defined an ``abstract`` type called ``Auditable`` to take advantage
of EdgeDB's schema mixin system. This allows us to add a ``created_at``
property to multiple types without repeating ourselves. Abstract types
don't have any concrete footprints in the database, as they don't hold any
actual data. Their only job is to propagate properties, links, and constraints
to the types that extend them.

The ``User`` type extends ``Auditable`` and inherits the ``created_at``
property as a result. Since ``created_at`` has a ``default`` value, it's
auto-filled with the return value of the ``datetime_current`` function. Along
with the property conveyed to it by the extended type, the ``User`` type
defines its own concrete required property called ``name``. We impose two
constraints on this property: names should be unique (``constraint exclusive``)
and shorter than 50 characters (``constraint max_len_value(50)``).

We also define an ``Event`` type that extends the ``Auditable`` abstract type.
It contains its own concrete properties and links: ``address``, ``schedule``,
and an optional link called ``host`` that corresponds to a ``User``.

Run a migration
===============

With the schema created, it's time to lock it in. The first step is to create a
migration.

.. code-block:: bash

    $ edgedb migration create

When this step is successful, you'll see
``Created dbschema/migrations/00001.edgeql``.

Now run the migration we just created.

.. code-block:: bash

    $ edgedb migrate

Once this is done, you'll see ``Applied`` along with the migration's ID. I like
to go one step further in verifying success and see the schema applied to my
database. To do that, first fire up the EdgeDB console:

.. code-block:: bash

    $ edgedb

In the console, type ``\ds`` (for "describe schema"). If everything worked, we
should output very close to the schema we added in the ``default.esdl`` file:

::

    module default {
        abstract type Auditable {
            required property created_at: std::datetime {
                default := (std::datetime_current());
                readonly := true;
            };
        };
        type Event extending default::Auditable {
            link host: default::User;
            property address: std::str;
            required property name: std::str {
                constraint std::exclusive;
                constraint std::max_len_value(50);
            };
            property schedule: std::datetime;
        };
        type User extending default::Auditable {
            required property name: std::str {
                constraint std::exclusive;
                constraint std::max_len_value(50);
            };
        };
    };

Build the API endpoints
=======================

With the schema established, we're ready to start building out the app. Let's
start by creating an ``app`` directory inside our project:

.. code-block:: bash

    $ mkdir app

Within this ``app`` directory, we're going to create three modules:
``events.py`` and ``users.py`` which represent the events and users APIs
respectively, and ``main.py`` that registers all the endpoints and exposes them
to the `uvicorn <https://www.uvicorn.org>`_ webserver. We also need an
``__init__.py`` to mark this directory as a package so we can easily import
from it. Go ahead and create that file now in your editor or via the command
line like this (from the project root):

.. code-block:: bash

    $ touch app/__init__.py

We'll work on the users API first since it's the simpler of the two.


Users API
^^^^^^^^^

We want this app to be type safe, end to end. To achieve this, instead of
hard-coding string queries into the app, we'll use code generation to generate
typesafe functions from queries we write in ``.edgeql`` files. These files are
simple text files containing the queries we want our app to be able to run.

The code generator will search through our project for all files with the
``.edgeql`` extension and generate those functions for us as individual Python
modules. When you installed the EdgeDB client (via ``pip install edgedb``), the
code generator was installed alongside it, so you're already ready to go. We
just need to write those queries!

We'll write queries for one endpoint at a time to start so you can see how the
pieces fit together. To keep things organized, create a new directory inside
``app`` called ``queries``. Create a new file in ``app/queries`` named
``get_users.edgeql`` and open it in your editor. Write the query into this
file. It's the same one we would have written inline in our Python code as
shown in the code block above:

.. code-block:: edgeql
    :caption: app/queries/get_users.edgeql

    select User {name, created_at};

We need one more query to finish off this endpoint. Create another file inside
``app/queries`` named ``get_user_by_name.edgeql`` and open it in your editor.
Add this query:

.. code-block:: edgeql

    select User {name, created_at}
    filter User.name = <str>$name

Save that file and get ready to kick off the magic that is code generation! 🪄

.. code-block:: bash

    $ edgedb-py
    Found EdgeDB project: <project-path>
    Processing <project-path>/app/queries/get_user_by_name.edgeql
    Processing <project-path>/app/queries/get_users.edgeql
    Generating <project-path>/app/queries/get_user_by_name.py
    Generating <project-path>/app/queries/get_users.py

The code generator creates one module per query file by default and places them
at the same path as the query files.

With code generated, we're ready to write an endpoint. Let's create the ``GET
/users`` endpoint so that we can request the ``User`` objects saved in the
database. Create a new file ``app/users.py``, open it in your editor, and add
the following code:

.. lint-off

.. code-block:: python
    :caption: app/users.py

    from __future__ import annotations

    import datetime
    from http import HTTPStatus
    from typing import List

    import edgedb
    from fastapi import APIRouter, HTTPException, Query
    from pydantic import BaseModel

    from .queries import get_user_by_name_async_edgeql as get_user_by_name_qry
    from .queries import get_users_async_edgeql as get_users_qry

    router = APIRouter()
    client = edgedb.create_async_client()


    class RequestData(BaseModel):
        name: str


    @router.get("/users")
    async def get_users(
        name: str = Query(None, max_length=50)
    ) -> List[get_users_qry.GetUsersResult] | get_user_by_name_qry.GetUserByNameResult:

        if not name:
            users = await get_users_qry.get_users(client)
            return users
        else:
            user = await get_user_by_name_qry.get_user_by_name(client, name=name)
            return user

.. lint-on

We've imported the generated code and aliased it (using ``as <new-name>``) to
make the module names we use in our code a bit neater.

The ``APIRouter`` instance does the actual work of exposing the API. We also
create an async EdgeDB client instance to communicate with the database.

By default, this API will return a list of all users, but you can also filter
the user objects by name. We have the ``RequestData`` class to handle the data
an API consumer will need to send in case they want to get only a single user.
The types we're using in the return annotation have been generated by the
EdgeDB code generation based on the queries we wrote and our database's schema.

Note that we're also calling the appropriate generated function based on
whether or not the API consumer passes an argument for ``name``.

This nearly gets us there but not quite. We have one potential outcome not
accounted for: a query for a user by name that returns no results. In that
case, we'll want to return a 404 (not found).

To fix it, we'll check in the else case whether we got anything back
from the single user query. If not, we'll go ahead and raise an exception. This
will send the 404 (not found) response to the user.

.. lint-off

.. code-block:: python
    :caption: app/users.py

    ...
    if not name:
        users = await get_users_qry.get_users(client)
        return users
    else:
        user = await get_user_by_name_qry.get_user_by_name(client, name=name)
        if not user:
            raise HTTPException(
                status_code=HTTPStatus.NOT_FOUND,
                detail={"error": f"Username '{name}' does not exist."},
            )
        return user
    ...

.. lint-on

To summarize, in the ``get_users`` function, we use our generated code to
perform asynchronous queries via the ``edgedb`` client. Then we return the
query results. Afterward, the JSON serialization part is taken care of by
FastAPI.

Before we can use this endpoint, we need to expose it to the server. We'll do
that in the ``main.py`` module. Create ``app/main.py`` and open it in your
editor. Here's the content of the module:

.. code-block:: python
    :caption: app/main.py

    from __future__ import annotations

    from fastapi import FastAPI
    from starlette.middleware.cors import CORSMiddleware

    from app import users

    fast_api = FastAPI()

    # Set all CORS enabled origins.
    fast_api.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )


    fast_api.include_router(users.router)

Here, we import everything we need, including our own ``users`` module
containing the router and endpoint logic for the users API. We instantiate the
API, give it a permissive CORS configuration, and give it the users router.

To test the endpoint, go to the project root and run:

.. code-block:: bash

    $ uvicorn app.main:fast_api --port 5001 --reload

This will start a ``uvicorn`` server and you'll be able to start making
requests against it. Earlier, we installed the
`HTTPx <https://www.python-httpx.org/>`_ client library to make HTTP requests
programmatically. It also comes with a neat command-line tool that we'll use to
test our API.

While the ``uvicorn`` server is running, bring up a new console. Activate your
virtual environment by running ``source myenv/bin/activate`` and run:

.. code-block:: bash

    $ httpx -m GET http://localhost:5001/users

You'll see the following output on the console:

::

    HTTP/1.1 200 OK
    date: Sat, 16 Apr 2022 22:58:11 GMT
    server: uvicorn
    content-length: 2
    content-type: application/json

    []

.. note::

    If you find yourself with a result you don't expect when making a request
    to your API, switch over to the uvicorn server console. You should find a
    traceback that will point you to the problem area in your code.

If you see this result, that means the API is working! It's not especially
useful though. Our request yields an empty list because the database is
currently empty. Let's create the ``POST /users`` endpoint in ``app/users.py``
to start saving users in the database. Before we do that though, let's go ahead
and create the new query we'll need.

Create and open ``app/queries/create_user.edgeql`` and fill it with this query:

.. code-block:: edgeql
    :caption: app/queries/create_user.edgeql

    select (insert User {
        name := <str>$name
    }) {
        name,
        created_at
    };

.. note::

    We're running our ``insert`` inside a ``select`` here so that we can return
    the ``name`` and ``created_at`` properties. If we just ran the ``insert``
    bare, it would return only the ``id``.

Save the file and run ``edgedb-py`` to generate the new function. Now,
we're ready to open ``app/users.py`` again and add the POST endpoint. First,
import the generated function for the new query:

.. code-block:: python
    :caption: app/users.py

    ...
    from .queries import create_user_async_edgeql as create_user_qry
    from .queries import get_user_by_name_async_edgeql as get_user_by_name_qry
    from .queries import get_users_async_edgeql as get_users_qry
    ...

Then write the endpoint to call that function:

.. lint-off

.. code-block:: python
    :caption: app/users.py

    ...
    @router.post("/users", status_code=HTTPStatus.CREATED)
    async def post_user(user: RequestData) -> create_user_qry.CreateUserResult:

        try:
            created_user = await create_user_qry.create_user(client, name=user.name)
        except edgedb.errors.ConstraintViolationError:
            raise HTTPException(
                status_code=HTTPStatus.BAD_REQUEST,
                detail={"error": f"Username '{user.name}' already exists."},
            )
        return created_user

.. lint-on

In the above snippet, we ingest data with the shape dictated by the
``RequestData`` model and return a payload of the query results. The
``try...except`` block gracefully handles the situation where the API consumer
might try to create multiple users with the same name. A successful request
will yield the status code HTTP 201 (created) along with the new user's
``id``, ``name``, and ``created_at`` as JSON.

To test it out, make a request as follows:

.. code-block:: bash

    $ httpx -m POST http://localhost:5001/users \
            --json '{"name" : "Jonathan Harker"}'

The output should look similar to this:

::

    HTTP/1.1 201 Created
    ...
    {
      "id": "53771f56-6f57-11ed-8729-572f5fba7ddc",
      "name": "Jonathan Harker",
      "created_at": "2022-04-16T23:09:30.929664+00:00"
    }

.. note::

    Since IDs are generated, your ``id`` values probably won't match the values
    in this guide. This is not a problem.

If you try to make the same request again, it'll throw an HTTP 400
(bad request) error:

::

    HTTP/1.1 400 Bad Request
    ...
    {
    "detail": {
      "error": "Username 'Jonathan Harker' already exists."
      }
    }

Before we move on to the next step, create 2 more users called
``Count Dracula`` and ``Mina Murray``. Once you've done that, we can move on to
the next step of building the ``PUT /users`` endpoint to update existing user
data.

We'll start again with the query. Create a new file in ``app/queries`` named
``update_user.edgeql``. Open it in your editor and enter this query:

.. code-block:: edgeql
    :caption: app/queries/update_user.edgeql

    select (
        update User filter .name = <str>$current_name
            set {name := <str>$new_name}
    ) {name, created_at};

Save the file and generate again using ``edgedb-py``. Now, we'll import that
and add the endpoint over in ``app/users.py``.

.. lint-off

.. code-block:: python
    :caption: app/users.py

    ...
    from .queries import create_user_async_edgeql as create_user_qry
    from .queries import get_user_by_name_async_edgeql as get_user_by_name_qry
    from .queries import get_users_async_edgeql as get_users_qry
    from .queries import update_user_async_edgeql as update_user_qry
    ...
    @router.put("/users")
    async def put_user(
        user: RequestData, current_name: str
    ) -> update_user_qry.UpdateUserResult:
        try:
            updated_user = await update_user_qry.update_user(
                client,
                new_name=user.name,
                current_name=current_name,
            )
        except edgedb.errors.ConstraintViolationError:
            raise HTTPException(
                status_code=HTTPStatus.BAD_REQUEST,
                detail={"error": f"Username '{user.name}' already exists."},
            )

        if not updated_user:
            raise HTTPException(
                status_code=HTTPStatus.NOT_FOUND,
                detail={"error": f"User '{current_name}' was not found."},
            )
        return updated_user

.. lint-on

Not much new happening here. We wrote our query with a ``current_name``
parameter for finding the user to be updated. The ``user`` argument will give
us the changes to make to that user, which in this case can only be the
``name`` since that's the only property a user has. We pull the name out of
``user`` and pass it as our ``new_name`` argument to the generated function.
The endpoint calls the generated function passing the client and those two
values, and the user is updated.

We've accounted for the possibility of a user trying to change a user's name to
a new name that conflicts with a different user. That will return a 400 (bad
request) error. We've also accounted for the possibility of a user trying to
update a user that doesn't exist, which will return a 404 (not found).

Let's save everything and test this out.

.. code-block:: bash

    $ httpx -m PUT http://localhost:5001/users \
            -p 'current_name' 'Jonathan Harker' \
            --json '{"name" : "Dr. Van Helsing"}'

This will return:

::

    HTTP/1.1 200 OK
    ...
    [
      {
        "id": "53771f56-6f57-11ed-8729-572f5fba7ddc",
        "name": "Dr. Van Helsing",
        "created_at": "2022-04-16T23:09:30.929664+00:00"
      }
    ]

If you try to change the name of a user to match that of an existing user, the
endpoint will throw an HTTP 400 (bad request) error:

.. code-block:: bash

    $ httpx -m PUT http://localhost:5001/users \
            -p 'current_name' 'Count Dracula' \
            --json '{"name" : "Dr. Van Helsing"}'

This returns:

::

    HTTP/1.1 400 Bad Request
    ...
    {
      "detail": {
        "error": "Username 'Dr. Van Helsing' already exists."
      }
    }

Since we've verified that endpoint is working, let's move on to the ``DELETE
/users`` endpoint. It'll allow us to query the name of the targeted object to
delete it.

Start by creating ``app/queries/delete_user.edgeql`` and filling it with this
query:

.. code-block:: edgeql
    :caption: app/queries/delete_user.edgeql

    select (
        delete User filter .name = <str>$name
    ) {name, created_at};

Generate the new function by again running ``edgedb-py``. Then re-open
``app/users.py``. This endpoint's code will look similar to the endpoints
we've already written:

.. lint-off

.. code-block:: python
    :caption: app/users.py

    ...
    from .queries import create_user_async_edgeql as create_user_qry
    from .queries import delete_user_async_edgeql as delete_user_qry
    from .queries import get_user_by_name_async_edgeql as get_user_by_name_qry
    from .queries import get_users_async_edgeql as get_users_qry
    from .queries import update_user_async_edgeql as update_user_qry
    ...
    @router.delete("/users")
    async def delete_user(name: str) -> delete_user_qry.DeleteUserResult:
        try:
            deleted_user = await delete_user_qry.delete_user(
                client,
                name=name,
            )
        except edgedb.errors.ConstraintViolationError:
            raise HTTPException(
                status_code=HTTPStatus.BAD_REQUEST,
                detail={"error": "User attached to an event. Cannot delete."},
            )

        if not deleted_user:
            raise HTTPException(
                status_code=HTTPStatus.NOT_FOUND,
                detail={"error": f"User '{name}' was not found."},
            )
        return deleted_user

.. lint-on

This endpoint will simply delete the requested user if the user isn't attached
to any event. If the targeted object *is* attached to an event, the API will
throw an HTTP 400 (bad request) error and refuse to delete the object. To
test it out by deleting ``Count Dracula``, on your console, run:

.. code-block:: bash

    $ httpx -m DELETE http://localhost:5001/users \
            -p 'name' 'Count Dracula'

If it worked, you should see this result:

::

    HTTP/1.1 200 OK
    ...
    [
      {
        "id": "e6837562-6f55-11ed-8744-ff1b295ed864",
        "name": "Count Dracula",
        "created_at": "2022-04-16T23:23:56.630101+00:00"
      }
    ]

With that, you've written the entire users API! Now, we move onto the events
API which is slightly more complex. (Nothing you can't handle though. 😁)

Events API
^^^^^^^^^^

Let's start with the ``POST /events`` endpoint, and then we'll fetch the
objects created via POST using the ``GET /events`` endpoint.

First, we need a query. Create a file ``app/queries/create_event.edgeql`` and
drop this query into it:

.. code-block:: edgeql
    :caption: app/queries/create_event.edgeql

    with name := <str>$name,
        address := <str>$address,
        schedule := <str>$schedule,
        host_name := <str>$host_name

    select (
        insert Event {
            name := name,
            address := address,
            schedule := <datetime>schedule,
            host := assert_single(
                (select detached User filter .name = host_name)
            )
        }
    ) {name, address, schedule, host: {name}};

Run ``edgedb-py`` to generate a function from that query.

Create a file in ``app`` named ``events.py`` and open it in your editor. It's
time to code up the endpoint to use that freshly generated query.

.. lint-off

.. code-block:: python
    :caption: app/events.py

    from __future__ import annotations

    from http import HTTPStatus
    from typing import List

    import edgedb
    from fastapi import APIRouter, HTTPException, Query
    from pydantic import BaseModel

    from .queries import create_event_async_edgeql as create_event_qry

    router = APIRouter()
    client = edgedb.create_async_client()


    class RequestData(BaseModel):
        name: str
        address: str
        schedule: str
        host_name: str


    @router.post("/events", status_code=HTTPStatus.CREATED)
    async def post_event(event: RequestData) -> create_event_qry.CreateEventResult:
        try:
            created_event = await create_event_qry.create_event(
                client,
                name=event.name,
                address=event.address,
                schedule=event.schedule,
                host_name=event.host_name,
            )

        except edgedb.errors.InvalidValueError:
            raise HTTPException(
                status_code=HTTPStatus.BAD_REQUEST,
                detail={
                    "error": "Invalid datetime format. "
                    "Datetime string must look like this: "
                    "'2010-12-27T23:59:59-07:00'",
                },
            )

        except edgedb.errors.ConstraintViolationError:
            raise HTTPException(
                status_code=HTTPStatus.BAD_REQUEST,
                detail=f"Event name '{event.name}' already exists,",
            )

        return created_event

.. lint-on

Like the ``POST /users`` endpoint, the incoming and outgoing shape of the
``POST /events`` endpoint's data are defined by the ``RequestData`` model and
the generated ``CreateEventResult`` model respectively. The ``post_events``
function asynchronously inserts the data into the database and returns the
fields defined in the ``select`` query we wrote earlier, along with the new
event's ``id``.

The exception handling logic validates the shape of the incoming data. For
example, just as before in the users API, the events API will complain if you
try to create multiple events with the same name. Also, the field ``schedule``
accepts data as an `ISO 8601 <https://en.wikipedia.org/wiki/ISO_8601>`_
timestamp string. Values not adhering to that will incur an HTTP 400 (bad
request) error.

It's almost time to test, but before we can do that, we need to expose this new
API in ``app/main.py``. Open that file, and update the import on line 6 to also
import ``events``:

.. code-block:: python
    :caption: app/main.py

    ...
    from app import users, events
    ...

Drop down to the bottom of ``main.py`` and include the events router:

.. code-block:: python
    :caption: app/main.py

    ...
    fast_api.include_router(events.router)

Let's try it out. Here's how you'd create an event:

.. code-block:: bash

    $ httpx -m POST http://localhost:5001/events \
            --json '{
                      "name":"Resuscitation",
                      "address":"Britain",
                      "schedule":"1889-07-27T23:59:59-07:00",
                      "host_name":"Mina Murray"
                    }'

If everything worked, you'll see output like this:

::

    HTTP/1.1 200 OK
    ...
    {
      "id": "0b1847f4-6f3d-11ed-9f27-6fcdf20ffe22",
      "name": "Resuscitation",
      "address": "Britain",
      "schedule": "1889-07-28T06:59:59+00:00",
      "host": {
        "name": "Mina Murray"
      }
    }

To speed this up a bit, we'll go ahead and write all the remaining queries in
one shot. Then we can flip back to ``app/events.py`` and code up all the
endpoints. Start by creating a file in ``app/queries`` named
``get_events.edgeql``. This one is really straightforward:

.. code-block:: edgeql
    :caption: app/queries/get_events.edgeql

    select Event {name, address, schedule, host : {name}};

Save that one and create ``app/queries/get_event_by_name.edgeql`` with this
query:

.. code-block:: edgeql
    :caption: app/queries/get_event_by_name.edgeql

    select Event {
        name, address, schedule,
        host : {name}
    } filter .name = <str>$name;

Those two will handle queries for ``GET /events``. Next, create
``app/queries/update_event.edgeql`` with this query:

.. code-block:: edgeql
    :caption: app/queries/update_event.edgeql

    with current_name := <str>$current_name,
        new_name := <str>$name,
        address := <str>$address,
        schedule := <str>$schedule,
        host_name := <str>$host_name

    select (
        update Event filter .name = current_name
        set {
            name := new_name,
            address := address,
            schedule := <datetime>schedule,
            host := (select User filter .name = host_name)
        }
    ) {name, address, schedule, host: {name}};

That query will handle PUT requests. The last method left is DELETE. Create
``app/queries/delete_event.edgeql`` and put this query in it:

.. code-block:: edgeql
    :caption: app/queries/delete_event.edgeql

    select (
        delete Event filter .name = <str>$name
    ) {name, address, schedule, host : {name}};

Run ``edgedb-py`` to generate the new functions. Open ``app/events.py``
so we can start getting these functions implemented in the API! We'll start by
coding GET. Import the newly generated queries and write the GET endpoint in
``events.py``:

.. lint-off

.. code-block:: python
    :caption: app/events.py

    ...
    from .queries import create_event_async_edgeql as create_event_qry
    from .queries import delete_event_async_edgeql as delete_event_qry
    from .queries import get_event_by_name_async_edgeql as get_event_by_name_qry
    from .queries import get_events_async_edgeql as get_events_qry
    from .queries import update_event_async_edgeql as update_event_qry
    ...
    @router.get("/events")
    async def get_events(
        name: str = Query(None, max_length=50)
    ) -> List[get_events_qry.GetEventsResult] | get_event_by_name_qry.GetEventByNameResult:
        if not name:
            events = await get_events_qry.get_events(client)
            return events
        else:
            event = await get_event_by_name_qry.get_event_by_name(client, name=name)
            if not event:
                raise HTTPException(
                    status_code=HTTPStatus.NOT_FOUND,
                    detail={"error": f"Event '{name}' does not exist."},
                )
            return event

.. lint-on

Save that file and test it like this:

.. code-block:: bash

    $ httpx -m GET http://localhost:5001/events

We should get back an array containing all our events (which, at the moment,
is just the one):

::

    HTTP/1.1 200 OK
    ...
    [
        {
            "id": "0b1847f4-6f3d-11ed-9f27-6fcdf20ffe22",
            "name": "Resuscitation",
            "address": "Britain",
            "schedule": "1889-07-28T06:59:59+00:00",
            "host": {
                "name": "Mina Murray"
            }
        }
    ]

You can also use the ``GET /events`` endpoint to return a single event object
by name. To locate the ``Resuscitation`` event, you'd use the ``name``
parameter with the GET API as follows:

.. code-block:: bash

    $ httpx -m GET http://localhost:5001/events \
            -p 'name' 'Resuscitation'

That'll return a result that looks like the response we just got without the
``name`` parameter, except that it's a single object instead of an array.

::

    HTTP/1.1 200 OK
    ...
    {
      "id": "0b1847f4-6f3d-11ed-9f27-6fcdf20ffe22",
      "name": "Resuscitation",
      "address": "Britain",
      "schedule": "1889-07-28T06:59:59+00:00",
      "host": {
        "name": "Mina Murray"
      }
    }

If we'd had multiple events, the response to our first test would have given us
all of them.

Let's finish off the events API with the PUT and DELETE endpoints. Open
``app/events.py`` and add this code:

.. lint-off

.. code-block:: python
    :caption: app/events.py

    ...
    @router.put("/events")
    async def put_event(
        event: RequestData, current_name: str
    ) -> update_event_qry.UpdateEventResult:
        try:
            updated_event = await update_event_qry.update_event(
                client,
                current_name=current_name,
                name=event.name,
                address=event.address,
                schedule=event.schedule,
                host_name=event.host_name,
            )

        except edgedb.errors.InvalidValueError:
            raise HTTPException(
                status_code=HTTPStatus.BAD_REQUEST,
                detail={
                    "error": "Invalid datetime format. "
                    "Datetime string must look like this: '2010-12-27T23:59:59-07:00'",
                },
            )

        except edgedb.errors.ConstraintViolationError:
            raise HTTPException(
                status_code=HTTPStatus.BAD_REQUEST,
                detail={"error": f"Event name '{event.name}' already exists."},
            )

        if not updated_event:
            raise HTTPException(
                status_code=HTTPStatus.INTERNAL_SERVER_ERROR,
                detail={"error": f"Update event '{event.name}' failed."},
            )

        return updated_event


    @router.delete("/events")
    async def delete_event(name: str) -> delete_event_qry.DeleteEventResult:
        deleted_event = await delete_event_qry.delete_event(client, name=name)

        if not deleted_event:
            raise HTTPException(
                status_code=HTTPStatus.INTERNAL_SERVER_ERROR,
                detail={"error": f"Delete event '{name}' failed."},
            )

        return deleted_event

.. lint-on

The events API is now ready to handle updates and deletion. Let's try out a
cool alternative way to test these new endpoints.


Browse the endpoints using the native OpenAPI doc
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

FastAPI automatically generates OpenAPI schema from the API endpoints and uses
those to build the API docs. While the ``uvicorn`` server is running, go to
your browser and head over to
`http://localhost:5001/docs <http://localhost:5001/docs>`_. You should see an
API navigator like this:

.. image::
    /docs/tutorials/fastapi/openapi.png
    :alt: FastAPI docs navigator
    :width: 100%

This documentation allows you to play with the APIs interactively. Let's try to
make a request to the ``PUT /events``. Click on the API that you want to try
and then click on the **Try it out** button. You can do it in the UI as
follows:

.. image::
    /docs/tutorials/fastapi/put.png
    :alt: FastAPI docs PUT events API
    :width: 100%

Clicking the **execute** button will make the request and return the following
payload:

.. image::
    /docs/tutorials/fastapi/put_result.png
    :alt: FastAPI docs PUT events API result
    :width: 100%

You can do the same to test ``DELETE /events``, just make sure you give it
whatever name you set for the event in your previous test of the PUT method.

Integrating EdgeDB Auth
=======================

EdgeDB Auth provides a built-in authentication solution that is deeply
integrated with the EdgeDB server. This section outlines how to enable and
configure EdgeDB Auth in your application schema, manage authentication
providers, and set key configuration parameters.

Setting up EdgeDB Auth
^^^^^^^^^^^^^^^^^^^^^^^

To start using EdgeDB Auth, you must first enable it in your schema. Add the
following to your schema definition:

.. code-block:: sdl

    using extension auth;   

Once added, make sure to apply the schema changes by migrating your database
schema.

.. code-block:: bash

    $ edgedb migration create
    $ edgedb migrate


Configuring EdgeDB Auth
-----------------------

The configuration of EdgeDB Auth involves setting various parameters to secure
and tailor authentication to your needs. For now, we'll focus on the essential
parameters to get started. You can configure these settings through a Python 
script, which is recommended for scalability, or you can use the EdgeDB UI for 
a more user-friendly approach.

**Auth Signing Key**

This key is used to sign the JWTs for internal operations. Although it's not
necessary for your application's functionality, it's essential for secure
token handling. To generate a secure key, you can use OpenSSL or Python with
the following commands:

Using OpenSSL:

.. code-block:: bash

    $ openssl rand -base64 32

Using Python:

.. code-block:: python

    import secrets
    print(secrets.token_urlsafe(32))

Once you have generated your key, configure it in EdgeDB like this:

.. code-block:: edgeql

    CONFIGURE CURRENT BRANCH SET
    ext::auth::AuthConfig::auth_signing_key := '<your-generated-key>';

**Allowed redirect URLs**

This configuration ensures that redirections are limited to domains under your
control. The ``allowed_redirect_urls`` setting specifies URLs that the Auth
extension can safely redirect to after authentication. A URL must exactly match
or be a sub-path of a URL in the list to be considered valid.

To configure this in your application:

.. code-block:: edgeql

    CONFIGURE CURRENT BRANCH SET
    ext::auth::AuthConfig::allowed_redirect_urls := {
        'http://localhost:8000',
        'http://localhost:8000/auth'
    };

Enabling authentication providers
---------------------------------

You need to configure at least one authentication provider to use EdgeDB Auth.
This can be done via the EdgeDB UI or directly through queries.

In this example, we'll configure a email and password provider. You can add 
it with the following query:

.. code-block:: edgeql

    CONFIGURE CURRENT BRANCH
    INSERT ext::auth::EmailPasswordProviderConfig {
        require_verification := false,
    };

.. note::

    ``require_verification`` defaults to ``true``. In this example, we're 
    setting it to ``false`` to simplify the setup. In a production environment, 
    you should set it to ``true`` to ensure that users verify their email 
    addresses before they can log in.

If you use the Email and Password provider, in addition to the
``require_verification`` configuration, you’ll need to configure SMTP to allow
EdgeDB to send email verification and password reset emails on your behalf.

Here is an example of setting a local SMTP server, in this case using a
product called `Mailpit <https://mailpit.axllent.org/docs/>`__ which is
great for testing in development:

.. code-block:: edgeql

    CONFIGURE CURRENT BRANCH SET
    ext::auth::SMTPConfig::sender := 'hello@example.com';

    CONFIGURE CURRENT BRANCH SET
    ext::auth::SMTPConfig::host := 'localhost';

    CONFIGURE CURRENT BRANCH SET
    ext::auth::SMTPConfig::port := <int32>1025;

    CONFIGURE CURRENT BRANCH SET
    ext::auth::SMTPConfig::security := 'STARTTLSOrPlainText';

    CONFIGURE CURRENT BRANCH SET
    ext::auth::SMTPConfig::validate_certs := false;

You can query the database configuration to discover which providers are 
configured with the following query:

.. code-block:: edgeql

  select cfg::Config.extensions[is ext::auth::AuthConfig].providers {
      name,
      [is ext::auth::OAuthProviderConfig].display_name,
  };

Implementing authentication with FastAPI
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Below, we provide a detailed guide to setting up authentication using FastAPI,
including both sign-in and sign-up functionalities.

PKCE flow for enhanced security
-------------------------------

The PKCE (Proof Key for Code Exchange) flow enhances security in server-to-server
authentication by generating a unique verifier and its corresponding challenge.
First, your server creates a 32-byte Base64 URL-encoded verifier, stores it in an
HttpOnly cookie, hashes it with SHA256, and then encodes it to form the challenge.

This implementation ensures enhanced security by preventing token leakage and is
tailored specifically for server-to-server interactions.

Add the following code to your FastAPI application to generate the PKCE:

.. code-block:: python
    :caption: app/auth.py

    import secrets
    import hashlib
    import base64

    def generate_pkce():
        verifier = secrets.token_urlsafe(32)
        challenge = hashlib.sha256(verifier.encode()).digest()
        challenge_base64 = base64.urlsafe_b64encode(challenge).decode('utf-8').rstrip('=')
        return verifier, challenge_base64

User registration and authentication
------------------------------------

Next, we're going to create endpoints in FastAPI to handle user registration
(sign-up) and user login (sign-in):

**Sign-up endpoint**

.. code-block:: python
    :caption: app/auth.py

    from fastapi import APIRouter, HTTPException, Request
    from fastapi.responses import JSONResponse
    import httpx

    router = APIRouter()
    
    # Value should be:
    # {protocol}://${host}:${port}/branch/${branch}/ext/auth/
    EDGEDB_AUTH_BASE_URL = os.getenv('EDGEDB_AUTH_BASE_URL')

    @router.post("/auth/signup")
    async def handle_signup(request: Request):
        body = await request.json()
        email = body.get("email")
        password = body.get("password")

        if not email or not password:
            raise HTTPException(status_code=400, detail="Missing email or password")

        verifier, challenge = generate_pkce()
        register_url = f"{EDGEDB_AUTH_BASE_URL}/register"
        register_response = httpx.post(register_url, json={
            "challenge": challenge,
            "email": email,
            "password": password,
            "provider": "builtin::local_emailpassword",
            "verify_url": "http://localhost:8000/auth/verify",
        })

        if register_response.status_code != 200 and register_response.status_code != 201:
            return JSONResponse(status_code=400, content={"message": "Registration failed"})

        code = register_response.json().get("code")
        token_url = f"{EDGEDB_AUTH_BASE_URL}/token"
        token_response = httpx.get(token_url, params={"code": code, "verifier": verifier})

        if token_response.status_code != 200:
            return JSONResponse(status_code=400, content={"message": "Token exchange failed"})

        auth_token = token_response.json().get("auth_token")

        response = JSONResponse(content={"message": "User registered"})
        response.set_cookie(key="edgedb-auth-token", value=auth_token, httponly=True, secure=True, samesite='strict')
        return response

The sign-up endpoint sends a POST request to the EdgeDB Auth server to register
a new user. It also sets the auth token as an HttpOnly cookie in the response.

**Sign-in endpoint**

.. code-block:: python
    :caption: app/auth.py

    @router.post("/auth/signin")
    async def handle_signin(request: Request):
        body = await request.json()
        email = body.get("email")
        password = body.get("password")
        provider = body.get("provider")

        if not email or not password or not provider:
            raise HTTPException(status_code=400, detail="Missing email, password, or provider.")

        verifier, challenge = generate_pkce()
        authenticate_url = f"{EDGEDB_AUTH_BASE_URL}/authenticate"
        response = httpx.post(authenticate_url, json={
            "challenge": challenge,
            "email": email,
            "password": password,
            "provider": provider,
        })

        if response.status_code != 200:
            return JSONResponse(status_code=400, content={"message": "Authentication failed"})

        code = response.json().get("code")
        token_url = f"{EDGEDB_AUTH_BASE_URL}/token"
        token_response = httpx.get(token_url, params={"code": code, "verifier": verifier})

        if token_response.status_code != 200:
            return JSONResponse(status_code=400, content={"message": "Token exchange failed"})

        auth_token = token_response.json().get("auth_token")
        response = JSONResponse(content={"message": "Authentication successful"})
        response.set_cookie(key="edgedb-auth-token", value=auth_token, httponly=True, secure=True, samesite='strict')
        return response

The sign-in endpoint sends a POST request to the EdgeDB Auth server to authenticate
a user. It then retrieves the code from the response and exchanges it for an auth 
token. The token is set as an HttpOnly cookie in the response.

**Add the auth endpoints to the FastAPI application**

Finally, add the auth endpoints to the FastAPI application:

.. code-block:: python-diff
    :caption: app/main.py

    + fast_api.include_router(events.router)

Creating a new user in the sign-up endpoint
-------------------------------------------

Now, let's automatically create a new user in the database when a user signs up.
We'll use the ``create_user_async_edgeql`` query we generated earlier 
to achieve this, but we'll need to modify it slightly to link it to the
EdgeDB Auth identity.

First, let's update the EdgeDB schema to include a new field in the User type
to store the EdgeDB Auth identity and a new ``current_user`` type.

.. code-block:: sdl-diff
    :caption: dbschema/default.esdl
    
    + global current_user := assert_single(
    +     ((
    +         select User
    +         filter .identity = global ext::auth::ClientTokenIdentity
    +     ))
    + );
      
      type User extending Auditable {
    +    required identity: ext::auth::Identity;
         required name: str {
            constraint exclusive;
            constraint max_len_value(50);
         };
      }

After updating the schema, run the following command to apply the changes:

.. code-block:: bash

    $ edgedb migration create
    $ edgedb migrate

Next, update the ``create_user_async_edgeql`` query to include the identity:

.. code-block:: edgeql-diff
    :caption: app/queries/create_user.edgeql

      select (
          insert User {
            name := <str>$name,
    +       identity := <ext::auth::Identity><uuid>$identity_id,
          }) {
          name,
          created_at,
      };

Run ``edgedb-py`` to generate the new function. Now, let's update the sign-up
endpoint to create a new user in the database. We need to do a few things:

1. Import ``edgedb``.

2. Create an EdgeDB client.

3. Get the identity ID from the EdgeDB Auth server response.

4. Create a new user in the database using the ``create_user_async_edgeql``
   query.


.. code-block:: python-diff

    + import edgedb
    + client = edgedb.create_async_client()

      @router.post("/auth/signup")
      async def handle_signup(request: Request):
          body = await request.json()
          email = body.get("email")
    +     name = body.get("name")
          password = body.get("password")

    -     if not email or not password:
    +     if not email or not password or not name:
    -         raise HTTPException(status_code=400, detail="Missing email or password.")
    +         raise HTTPException(status_code=400, detail="Missing email, password, or name.")
      
          verifier, challenge = generate_pkce()
          register_url = f"{EDGEDB_AUTH_BASE_URL}/register"
          register_response = httpx.post(register_url, json={
              "challenge": challenge,
              "email": email,
              "password": password,
              "provider": "builtin::local_emailpassword",
              "verify_url": "http://localhost:8000/auth/verify",
          })
      
          if register_response.status_code != 200 and register_response.status_code != 201:
              return JSONResponse(status_code=400, content={"message": "Registration failed"})
          
          code = register_response.json().get("code")
          token_url = f"{EDGEDB_AUTH_BASE_URL}/token"
          token_response = httpx.get(token_url, params={"code": code, "verifier": verifier})
      
          if token_response.status_code != 200:
              return JSONResponse(status_code=400, content={"message": "Token exchange failed"})

          auth_token = token_response.json().get("auth_token")
    +     identity_id = token_response.json().get("identity_id")
    +     try:
    +         created_user = await create_user_qry.create_user(client, name=name, identity_id=identity_id)
    +     except edgedb.errors.ConstraintViolationError:
    +         raise HTTPException(
    +             status_code=400,
    +             detail={"error": f"User with email '{email}' already exists."},
    +         )
              
          response = JSONResponse(content={"message": "User registered"})
          response.set_cookie(key="edgedb-auth-token", value=auth_token, httponly=True, secure=True, samesite='strict')
          return response

You can now test the sign-up endpoint by sending a POST request to
``http://localhost:8000/auth/signup`` with the following payload:

.. code-block:: json

    {
        "email": "jonathan@example.com",
        "name": "Jonathan Harker",
        "password": "password"
    }

If the request is successful, you should see a response with the message
``User registered``.
 

Wrapping up
===========

Now you have a fully functioning events API in FastAPI backed by EdgeDB. If you
want to see all the source code for the completed project, you'll find it in
`our examples repo
<https://github.com/edgedb/edgedb-examples/tree/main/fastapi-crud>`_. We also 
have a separate example that demonstrates how to integrate EdgeDB Auth with
FastAPI in the same repo. Check it out 
`here <https://github.com/edgedb/edgedb-examples/tree/main/fastapi-crud-auth>`_.
If you're stuck or if you just want to show off what you've built, come talk 
to us `on Discord <https://discord.gg/umUueND6ag>`_. It's a great community of 
helpful folks, all passionate about being part of the next generation of 
databases.

If you like what you see and want to dive deeper into EdgeDB and what it can
do, check out our `Easy EdgeDB book </easy-edgedb>`_. In
it, you'll get to learn more about EdgeDB as we build an imaginary role-playing
game based on Bram Stoker's Dracula.
