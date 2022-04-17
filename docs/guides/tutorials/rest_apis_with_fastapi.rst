.. _ref_guide_rest_apis_with_fastapi:

=====================================
Build REST APIs with EdgeDB & FastAPI
=====================================

EdgeDB can help you quickly build REST APIs in Python without having to deal with the rigmarole of using ORM libraries to effectively communicate with your database. Here, we'll be using `FastAPI <https://fastapi.tiangolo.com/>`_ to expose the API endpoints and EdgeDB to store the content.

We'll build a simple event management system where you'll be able to fetch, create, update, and delete *event hosts* and *events* via RESTful API endpoints.

Prerequisites
=============

Before we start, make sure you've :ref:`installed <ref_admin_install>` EdgeDB and EdgeDB-CLI. In this tutorial, we'll use Python 3.10 and take advantage of the asynchronous I/O paradigm to communicate with the database more efficiently. A working version of this tutorial can be found `here <https://github.com/edgedb/edgedb-examples/tree/main/fastapi-crud>`_ on GitHub.


Install the dependencies
^^^^^^^^^^^^^^^^^^^^^^^^

To follow along, clone the repository and head over to the ``fastapi-crud`` directory. Create a Python 3.10 virtual environment, activate it, and install the dependencies with this command:

.. code-block:: bash
    pip install edgedb fastapi httpx[cli] uvicorn


Initialize the database
^^^^^^^^^^^^^^^^^^^^^^^

Now, let's create a new EdgeDB instance for this project. From the root directory, run:

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

Once you've answered the prompts, a new EdgeDB instance called ``fastapi_crud`` will be created. Start the server with the following command:

.. code-block:: bash
    edgedb instance start fastapi_crud

Connect to the database
^^^^^^^^^^^^^^^^^^^^^^^

Let's test that we can connect to the newly started instance. To do so, run:

.. code-block:: bash
    edgedb instance start fastapi_crud

You should be connected to the database instance and able to see a prompt similar to this:

::
    EdgeDB 1.2+5aecabc (repl 1.1.1+5bb8bad)
    Type \help for help, \quit to quit.
    edgedb>

You can start writing queries here. However, we haven't talked about the shape of our data yet.

Schema design
=============

The event management system will have two entities—**events** and **users**. Each *event* can have an optional link to a *user*. The goal is to create API endpoints that'll allow us to fetch, create, update, and delete the entities while maintaining their relationships.

EdgeDB allows us to declaratively define the structure of the entities. If you've worked with SQLAlchemy or Django ORM, you might refer to these declarative schema definitions as *models*. By default, EdgeDB CLI looks for these schema definitions in the `dbschema/default.esdl` module. You can also create new modules in this directory and call the types from one module to another. However, for now, we'll define our entities in the `default.esdl` module. This is how our datatypes look:

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

Here, we've defined an abstracted type called ``AuditLog`` to take advantage of EdgeDB's polymorphic type system. This allows us to add a ``created_at`` property to multiple types without repeating ourselves. Also, abstract types don't have any concrete footprints in the database as they don't hold any actual data. Their only job is to propagate properties, links, and constraints to the types that extend them.

The ``User`` type extends ``AuditLog`` and inherits the ``created_at`` property as a result. This property is auto-filled by the abstract type via the ``datetime_current`` function. The datetime is saved as a UTC timestamp. Type User also has an annotation field. Annotations allow us to attach arbitrary descriptions to the types. Along with the inherited type, the user type also defines a concrete required property called ``name``. We impose two constraints on this property—names should be unique and they can't be longer than 50 characters.

Similar to the ``User`` type, we define an ``Event`` type that extends the ``AuditLog`` abstract type. An event will also have a ``name`` property and a few additional concrete properties like ``address`` and ``schedule``. While ``address`` holds string data, ``schedule`` expects the incoming data to be formatted as datetime. An ``Event`` can also have an optional link to a ``User``. This user here represents the host of an event. Currently, we're only allowing a single host to be attached to an event.


Build the API endpoints
=======================

The API endpoints are defined in the `app` directory. The directory structure looks as follows:

::
    app
    ├── __init__.py
    ├── events.py
    ├── main.py
    └── users.py

The `user.py` and `event.py` modules contain the code to build the ``User`` and ``Event`` APIs respectively. The ``main.py`` module then registers all the endpoints and exposes them to the `uvicorn <https://www.uvicorn.org/>`_ webserver.


User APIs
^^^^^^^^^^

Since the ``User`` type is simpler among the two, we'll start with that. Let's create a `GET users/` endpoint so that we can start looking at the user objects saved in the database. You can create the API with a couple of lines of code in FastAPI:

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
            ResponseData(name=user.name,
            created_at=user.created_at) for user in users
        )
        return response

The `APIRouter` instance does the actual work of exposing the API. We also create an async EdgeDB client instance to communicate with the database. By default, this API will return a list of users but you can also filter the objects by name. Since names are unique in this case, it guarantees that you'll get a single object in return whenever you filter by a name.

In the ``get_users`` function, we perform asynchronous queries via the ``edgedb`` client and serialize the returned data with the ``ResponseData`` model. Then we aggregate the instances in a generator and return it. Afterward, the JSON serialization part is taken care of by FastAPI. This endpoint is exposed to the server in the ``main.py`` module. Here's the content of the module:

.. code-block:: python
    # fastapi-crud/app/main.py

    from __future__ import annotations

    from fastapi import FastAPI
    from starlette.middleware.cors import CORSMiddleware

    from app import events, users

    fast_api = FastAPI()

    # Set all CORS enabled origins.
    fast_api.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )


    fast_api.include_router(events.router)
    fast_api.include_router(users.router)


To test the endpoint, go to the ``fastapi-crud`` directory and run:

.. code-block:: bash
    uvicorn app.main:fast_api --port 5000 --reload

This will start a ``uvicorn`` server and you'll be able to start making requests against it. Earlier, we installed the `HTTPx <https://www.python-httpx.org/>`_ client library to make HTTP requests programmatically. It also comes with a neat command-line tool that we'll use to test our API. While the ``uvicorn`` server is running, on a new console, run:

.. code-block:: bash
    httpx -m GET http://localhost:5000/users

You'll see the following output on the console:

::
    HTTP/1.1 200 OK
    date: Sat, 16 Apr 2022 22:58:11 GMT
    server: uvicorn
    content-length: 2
    content-type: application/json

    []

Our request yielded an empty list because the database doesn't have any object at this point. Let's create the ``POST /users`` endpoint to start saving users in the database. The POST endpoint can be built similarly:

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

In the above snippet, we ingest data with the shape dictated by the ``RequestData`` model and return a payload with the shape defined in the ``ResponseData`` model. The ``try...except`` block gracefully handles the situation where the API consumer might try to create multiple users with the same name. A successful request will yield the status code HTTP 201 (created). To test it out, make a request as follows:

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

If you try to make the same request again, it'll throw an HTTP 400 (bad request) error:

::
    HTTP/1.1 400 Bad Request
    ...
    {
    "detail": {
      "error": "Username 'Jonathan Harker' already exists."
      }
    }

Before we move on to the next step, create 2 more users called ``Count Dracula`` and ``Mina Murray``. Once you've done that, we can move on to the next step of building the ``PUT /users`` endpoint to update the user data. It can be built like this:


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

Here, we'll isolate the intended object that we want to update by filtering the users with the ``filter_name`` parameter. For example, if you wanted to update the properties of ``Jonathan Harker``, the value of the ``filter_name`` query parameter would be ``Jonathan Harker``. The following command changes the name of ``Jonathan Harker`` to ``Dr. Van Helsing``.

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
    async def delete_user(name: str) -> Iterable[ResponseData]:
        try:
            deleted_users = await client.query(
                """SELECT (
                    DELETE User FILTER .name=<str>$name
                ) {name, created_at};
                """,
                name=name,
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

This endpoint will simply delete the requested user if the user isn't attached to any event. If the targeted object is attached to an event, the API will throw an HTTP 400 (bad request) error and refuse to delete the object. To delete `Count Dracula`, on your console, run:

.. code-block:: bash
    httpx -m DELETE http://localhost:5000/users -p 'name' 'Count Dracula'

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

The event APIs are built in a similar manner as the user APIs. Without sounding too repetitive, let's look at how the ``POST /events`` endpoint is created and then we'll introspect the objects created with this API via the ``GET /events`` endpoint.

Take a look at how the POST API is built:


.. code-block:: python
    # fastapi-crud/app/events.py

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


    @router.post("/events", status_code=HTTPStatus.CREATED)
    async def post_event(event: RequestData) -> ResponseData:
        try:
            (created_event,) = await client.query(
                """
                WITH name:=<str>$name, address:=<str>$address,
                schedule:=<str>$schedule, host_name:=<str>$host_name

                SELECT (
                    INSERT Event {
                    name:=name,
                    address:=address,
                    schedule:=<datetime>schedule,
                    host:=assert_single((SELECT DETACHED User FILTER .name=host_name))
                }) {name, address, schedule, host: {name}};
                """,
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
                detail=f"Event name '{event.name}' already exists,",
            )

        return ResponseData(
            name=created_event.name,
            address=created_event.address,
            schedule=created_event.schedule,
            host=Host(name=created_event.host.name) if created_event.host else None,
        )

Like the ``POST /users`` API, here, the incoming and outgoing shape of the data is defined by the ``RequestData`` and ``ResponseData``models respectively. The ``post_events`` function asynchronously inserts the data into the database and returns the fields defined in the ``SELECT`` statement. EdgeQL allows us to perform insertion and selection of data fields at the same time. The exception handling logic validates the shape of the incoming data. For example, just as before, this API will complain if you try to create multiple events with the same. Also, the field ``schedule`` accepts data as an `ISO 8601 <https://en.wikipedia.org/wiki/ISO_8601>` timestamp string. Failing to do so will incur an HTTP 400 (bad request) error.

Here's how you'd create an event:


.. code-block:: bash
    httpx -m POST http://localhost:5000/events \
          --json '{"name":"Resuscitation", "address":"Britain", "schedule":"1889-07-27T23:59:59-07:00", "host_name":"Mina Murray"}'

That'll return:

::
    HTTP/1.1 200 OK
    ...
    {
      "name": "Resuscitation",
      "address": "Britain",
      "schedule": "1889-07-28T06:59:59+00:00",
      "host": {
        "name": "Mina Murray"
      }
    }

You can also use the ``GET /events`` endpoint to list and filter the event objects. To locate the ``Resuscitation`` event, you'd use the ``filter_name`` parameter with the GET API as follows:

.. code-block:: bash
    httpx -m GET http://localhost:5000/events -p 'name' 'Resuscitation'

That'll return:

::
    HTTP/1.1 200 OK
    ...
    {
      "name": "Resuscitation",
      "address": "Britain",
      "schedule": "1889-07-28T06:59:59+00:00",
      "host": {
        "name": "Mina Murray"
      }
    }

Take a look at the ``app/events.py`` file to see how the ``PUT /events`` and ``DELETE /events`` endpoints are constructed.


Browse the endpoints using the native OpenAPI docs
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

FastAPI automatically generates OpenAPI schema from the API endpoints and uses those to build the API docs. While the ``uvicorn`` server is running, go to your browser and head over to `http://localhost:5000/docs <http://locahost:5000/docs>`_. You should see an API navigator like this:

.. image:: https://user-images.githubusercontent.com/30027932/163730215-ddd4d870-ec64-41bb-8284-6aaa73353bec.png
  :width: 600
  :alt: FastAPI docs navigator

The doc allows you to play with the APIs interactively. Let's try to make a request to the ``PUT /events``. Click on the API that you want to try and then click on the **Try it out** button. You can do it in the UI as follows:


.. image:: https://user-images.githubusercontent.com/30027932/163731004-5222e785-7f3d-47d3-b33a-beaccdd65b74.png
  :width: 600
  :align: center
  :alt: FastAPI docs PUT events API

Clicking the **execute** button will make the request and return the following payload:


.. image:: https://user-images.githubusercontent.com/30027932/163731082-aadbc873-25f4-4df6-bff6-a9f23b352a08.png
  :width: 600
  :align: center
  :alt: FastAPI docs PUT events API
