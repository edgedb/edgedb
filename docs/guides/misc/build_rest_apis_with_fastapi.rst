.. _ref_guide_build_rest_apis_with_fastapi:

=====================================
Build REST APIs with EdgeDB & FastAPI
=====================================

EdgeDB can help you quickly build REST APIs in Python without having to deal with the idiosyncrasies of ORM libraries to effectively communicate with your database. Here, we'll be using `FastAPI <https://fastapi.tiangolo.com/>`_ to expose the API endpoints and EdgeDB to store the content.

We'll build a simple event management system where you'll be able to fetch, create, update, and delete *events* via RESTful API endpoints.

Prerequisites
=============

Before we start, make sure you've :ref:`installed <ref_admin_install>` EdgeDB and EdgeDB-CLI. In this tutorial, we'll use Python 3.10 and take advantage of asynchronous I/O to make communication with the database more efficient. A working version of this tutorial can be found `here <https://github.com/edgedb/edgedb-examples/tree/main/fastapi-crud>`_ on GitHub.


Install the dependencies
^^^^^^^^^^^^^^^^^^^^^^^^

To follow along, clone the repository and head over to the ``fastapi-crud`` directory. Create a Python 3.10 virtual environment, activate it, and install the dependencies:

.. code-block:: bash
    pip install edgedb fastapi httpx[cli] uvicorn


Initialize the database
^^^^^^^^^^^^^^^^^^^^^^^

Now, let's create a new EdgeDB instance for this project. Run:

.. code-block:: bash
    edgedb project init

You should see the following prompts on your console:

::
    Initializing project...

    Specify the name of EdgeDB instance to use with this project
    [default: fastapi_crud]:
    > fastapi_crud

    Do you want to start instance automatically on login? [y/n]
    > n
    Checking EdgeDB versions...

Once you've answered the prompts, a new EdgeDB called ``fastapi_crud`` will be created. Start the server with the following command:

.. code-block:: bash
    edgedb instance start fastapi_crud

Connect to the database
^^^^^^^^^^^^^^^^^^^^^^^

Let's test that we can connect to the newly started instance. To do so, run:

.. code-block:: bash
    edgedb instance start fastapi_crud

You should be connected to the database instance and able see a prompt similar to this:

::
    EdgeDB 1.2+5aecabc (repl 1.1.1+5bb8bad)
    Type \help for help, \quit to quit.
    edgedb>

You can start writing queries here. However, we haven't talked about the shape of our data yet.

Schema design
=============

Our event management system will have two entities—**events** and **users**. Each *event* can have an optional link to a *user*. The goal is to create API endpoints that'll allow us to fetch, create, update, and delete the entities while maintaining their relationships.

EdgeDB allows us to declaratively define the structure of the entities. If you've worked with SQLAlchemy or Django ORM, you might refer to this declarative schema definition as *models*. By default, EdgeDB stores these schema definiton in the `dbschema/default.esdl` module. You can also create new modules and call the types from one module to another. However, for now, we'll define our entities in the `default.esdl` module. This is how our datatypes look:

.. code-block:: esdl
    # dbschema/default.esdl

    module default {
    abstract type AuditLog {
      annotation description := "Add 'create_at' and 'update_at' properties to all types.";
      property created_at -> datetime {
        default := datetime_current();
      }
    }

    type User extending AuditLog {
      annotation description := "Event host.";
      required property name -> str {
        constraint exclusive;
        constraint max_len_value(50);
      };
    }

    type Event extending AuditLog {
      annotation description := "Some grand event.";
      required property name -> str {
        constraint exclusive;
        constraint max_len_value(50);
      }
      property address -> str;
      property schedule -> datetime;
      link host -> User;
    }
    }

Here, we've defined an ``AuditLog`` abstract type to take advantage of EdgeDB's polymorphic type system. This allows us add a ``created_at`` property to every other type without repeating ourselves. Also, abstract types don't have any concrete footprints on the database as they don't hold any actual data. Their only job is to propagate properties, links, and constraints to the types that extend them.

The ``User`` type extends ``AuditLog`` and inherits the ``created_at`` property as a result. This property is auto-filled by the abstract class via the ``datetime_current`` function. The datetime is saved as a UTC timestamp. User type also has an annotation field. Annotations allows us to attach arbitrary description to the types. Along with the inherited type, the user type also defines a concrete required property called ``name``. We impose two constraints on this property—names should be unique and they can't be longer than 50 characters.

Similar to the ``User`` type, we define an ``Event`` type that extends the ``AuditLog`` abstract type. An event will also have a name property and a few additional concrete properties like ``address`` and ``schedule``. While ``address`` holds string data, ``schedule`` expects the incoming data to be formatted in datetime format. An ``Event`` can also have an optional link to a ``User``. This user here represents the host of an event. Currently, we're only allowing a single host attached to an event.


Build the API endpoints
=======================

The API endpoints are defined in the `app` directory. The directory structure looks as follows:

::
    app
    ├── __init__.py
    ├── events.py
    ├── main.py
    └── users.py

The `user.py` and `event.py` modules houses the code to build the ``User`` and ``Event`` APIs respectively. The ``main.py`` module then aggregrates all the endpoints and exposes them to the ``uvicorn`` webserver.


User APIs
^^^^^^^^^

Since the ``User`` type is the simpler one among the two, we'll start with that. Let's create a `GET users/` endpoint first, so that we can start looking at the objects saved in the database. You can create the API with a couple of lines of code in FastAPI:

.. code-block:: python
    # fastapi-crud/app/users.py

    from __future__ import annotations

    import datetime
    from http import HTTPStatus
    from typing import Iterable

    import edgedb
    from fastapi import APIRouter, HTTPException, Query
    from pydantic import BaseModel

    router = APIRouter()
    client = edgedb.create_async_client()


    class RequestData(BaseModel):
        name: str


    class ResponseData(BaseModel):
        name: str
        created_at: datetime.datetime


    @router.get("/users")
    async def get_users(
        name: str = Query(None, max_length=50)
        ) -> Iterable[ResponseData]:

        if not name:
            users = await client.query("SELECT User {name, created_at};")
        else:
            users = await client.query(
                """SELECT User {name, created_at} FILTER User.name=<str>$name""",
                name=name,
            )
        response = (
            ResponseData(name=user.name, created_at=user.created_at) for user in users
        )
        return response

The `APIRouter` instance does the actual work of exposing the API. We also create an async edgedb client instance to communicate with our the database. By default, this API will return a list of users but you can also filter objects by name. Since names are unique in this case, it guarantees that you'll get a single object in return whenever you filter by name.

In the ``get_users`` function, we perform asynchronous queries via the ``edgedb`` client and serialize the returned data with the ``ResponseData`` model. Then we aggregate the instances in a generator and return it and the rest is taken care of by FastAPI. This endpoint is exposed to the server in the ``main.py`` module. Here's the content of that:

.. code-block:: python
    # fastapi-crud/app/main.py

    from __future__ import annotations

    from fastapi import FastAPI
    from starlette.middleware.cors import CORSMiddleware

    from app import events, users

    fast_api = FastAPI()

    # Set all CORS enabled origins
    fast_api.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )


    fast_api.include_router(events.router)
    fast_api.include_router(users.router)


The `main.py` module registers the endpoints to the application instance. To test the endpoint, go to the ``fastapi-crud`` directory and run:

.. code-block:: bash
    uvicorn app.main:fast_api --port 5000 --reload

This will start a ``uvicorn`` server and you'll be able to start making request against it. Earlier, we've already installed the `HTTPx <https://www.python-httpx.org/>`_ client library to make HTTP requests programatically. It also comes with neat CLI tool that we'll use to test our API. While the server is running, on a new console, run:

.. code-block:: bash
    uvicorn app.main:fast_api --port 5000 --reload

You'll see the following output on the console:

::
    HTTP/1.1 200 OK
    date: Sat, 16 Apr 2022 22:58:11 GMT
    server: uvicorn
    content-length: 2
    content-type: application/json

    []

This returns an empty list because our database doesn't have any objects at this point. Let's create the ``POST /users`` endpoint to create data points. The POST endpoint can be built similarly:

.. code-block:: python
    # fastapi-crud/app/users.py

    ...
    @router.post("/users", status_code=HTTPStatus.CREATED)
    async def post_user(user: RequestData) -> ResponseData:

        try:
            (created_user,) = await client.query(
                """SELECT (INSERT User {name:=<str>$name}) {name, created_at};""",
                name=user.name,
            )
        except edgedb.errors.ConstraintViolationError:
            raise HTTPException(
                status_code=HTTPStatus.BAD_REQUEST,
                detail={"error": f"Username '{user.name}' already exists,"},
            )
        response = ResponseData(name=created_user.name, created_at=created_user.created_at)
        return response

In the above snippet, we ingest data with the shape dictated by the ``RequestData`` model and return payload with the shape defined in the ``ResponseData`` model. The ``try...except`` block gracefully handles the situtation where the API consumer might try to create another user with the same name. The response will appear with the status code HTTP 201 (created). To test it out, make a request as follows:

.. code-block:: bash
    httpx -m POST http://localhost:5000/users --json '{"name" : "Jonathan Harker"}'


The output should look similar to this:

::
    HTTP/1.1 201 Created
    ...
    {
      "name": "Jonathan Harker",
      "created_at": "2022-04-16T23:09:30.929664+00:00"
    }

If you try to make the same request again, it'll throw an error:

::
    HTTP/1.1 400 Bad Request
    ...
    {
    "detail": {
      "error": "Username 'Jonathan Harker' already exists."
      }
    }

Before we move on to the next step, create 2 more users called ``Count Dracula`` and ``Mina Murray``. Once you've done that, we can move onto the next step of builing a ``PUT /users`` endpoint to update the user data. It can be built like this:


.. code-block:: python
    # fastapi-crud/app/users.py

    @router.put("/users")
    async def put_user(user: RequestData, filter_name: str) -> Iterable[ResponseData]:
        try:
            updated_users = await client.query(
                """
                SELECT (
                    UPDATE User FILTER .name=<str>$filter_name
                    SET {name:=<str>$name}
                ) {name, created_at};
                """,
                name=user.name,
                filter_name=filter_name,
            )
        except edgedb.errors.ConstraintViolationError:
            raise HTTPException(
                status_code=HTTPStatus.BAD_REQUEST,
                detail={"error": f"Username '{filter_name}' already exists."},
            )
        response = (
            ResponseData(name=user.name, created_at=user.created_at)
            for user in updated_users
        )
        return response

Here, first we locate the intended object by filtering the users with a ``filter_name``. For example, if you wanted to update the properties of ``Jonathan Harker``, the value of the ``filter_name`` query parameter would be ``Jonathan Harker``. The following command change the name of ``Jonathan Harker`` to ``Dr. Van Helsing``.

.. code-block:: bash
    httpx -m PUT http://localhost:5000/users -p 'filter_name' 'Jonathan Harker' \
          --json '{"name" : "Dr. Van Helsing"}'

This will return:

::
    HTTP/1.1 200 OK
    ...
    [
      {
        "name": "Dr. Van Helsing",
        "created_at": "2022-04-16T23:09:30.929664+00:00"
      }
    ]

If you try to change the name of a user to match that of an existing user, the endpoint will throw an HTTP 400 (bad request) error:

.. code-block:: bash
    httpx -m PUT http://localhost:5000/users -p 'filter_name' 'Count Dracula' \
          --json '{"name" : "Dr. Van Helsing"}'

This returns:

::
    HTTP/1.1 400 Bad Request
    ...
    {
      "detail": {
        "error": "Username 'Count Dracula' already exists."
      }
    }

Another API that we'll need to cover is the ``DELETE /users`` endpoint. It'll allow us to query the name of the targeted object and delete that. The code looks similar to the ones you've already seen:


.. code-block:: python
    # fastapi-crud/app/users.py

    @router.delete("/users")
    async def delete_user(filter_name: str) -> Iterable[ResponseData]:
        try:
            deleted_users = await client.query(
                """SELECT (
                    DELETE User FILTER .name=<str>$filter_name
                ) {name, created_at};
                """,
                filter_name=filter_name,
            )
        except edgedb.errors.ConstraintViolationError:
            raise HTTPException(
                status_code=HTTPStatus.BAD_REQUEST,
                detail={"error": "User attached to an event. Cannot delete."},
            )

        response = (
            ResponseData(name=deleted_user.name, created_at=deleted_user.created_at)
            for deleted_user in deleted_users
        )

        return response

This endpoint will simply delete the requested user if the user isn't attached to any event. In that case, it'll throw an HTTP 400 (bad request) error and refuse to delete the object. To delete `Count Dracula`, on your console, run:

.. code-block:: bash
    httpx -m DELETE http://localhost:5000/users -p 'filter_name' 'Count Dracula'

That'll return:

::
    HTTP/1.1 200 OK
    ...
    [
      {
        "name": "Count Dracula",
        "created_at": "2022-04-16T23:23:56.630101+00:00"
      }
    ]

Event APIs
^^^^^^^^^^


Browse the APIs using the native OpenAPI docs
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
