.. _ref_guide_rest_apis_with_flask:

=====
Flask
=====

:edb-alt-title: Building a REST API with EdgeDB and Flask

The EdgeDB Python client makes it easy to integrate EdgeDB into your preferred
web development stack. In this tutorial, we'll see how you can quickly start
building RESTful APIs with `Flask <https://flask.palletsprojects.com>`_ and
EdgeDB.

We'll build a simple movie organization system where you'll be able to fetch,
create, update, and delete *movies* and *movie actors* via RESTful API
endpoints.

Prerequisites
=============

Before we start, make sure you've :ref:`installed <ref_admin_install>` the
``edgedb`` command-line tool. Here, we'll use Python 3.10 and a few of its
latest features while building the APIs. A working version of this tutorial can
be found `on Github
<https://github.com/edgedb/edgedb-examples/tree/main/flask-crud>`_.


Install the dependencies
^^^^^^^^^^^^^^^^^^^^^^^^

To follow along, clone the repository and head over to the ``flask-crud``
directory.


.. code-block:: bash

    $ git clone git@github.com:edgedb/edgedb-examples.git
    $ cd edgedb-examples/flask-crud

Create a Python 3.10 virtual environment, activate it, and install the
dependencies with this command:

.. code-block:: bash

    $ python -m venv myvenv
    $ source myvenv/bin/activate
    $ pip install edgedb flask 'httpx[cli]'


Initialize the database
^^^^^^^^^^^^^^^^^^^^^^^

Now, let's initialize an EdgeDB project. From the project's root directory:

.. code-block:: bash

    $ edgedb project init
    Initializing project...

    Specify the name of EdgeDB instance to use with this project
    [default: flask_crud]:
    > flask_crud

    Do you want to start instance automatically on login? [y/n]
    > y
    Checking EdgeDB versions...

Once you've answered the prompts, a new EdgeDB instance called ``flask_crud``
will be created and started.


Connect to the database
^^^^^^^^^^^^^^^^^^^^^^^

Let's test that we can connect to the newly started instance. To do so, run:

.. code-block:: bash

    $ edgedb

You should be connected to the database instance and able to see a prompt
similar to this:

::

    EdgeDB 2.x (repl 2.x)
    Type \help for help, \quit to quit.
    edgedb>

You can start writing queries here. However, the database is currently
empty. Let's start designing the data model.

Schema design
=============

The movie organization system will have two object types—**movies** and
**actors**. Each *movie* can have links to multiple *actors*. The goal is to
create API endpoints that'll allow us to fetch, create, update, and delete the
objects while maintaining their relationships.

EdgeDB allows us to declaratively define the structure of the objects. The
schema lives inside ``.esdl`` file in the ``dbschema`` directory. It's
common to declare the entire schema in a single file ``dbschema/default.esdl``.
This is how our datatypes look:

.. code-block:: sdl

    # dbschema/default.esdl

    module default {
      abstract type Auditable {
        property created_at -> datetime {
          readonly := true;
          default := datetime_current();
        }
      }

      type Actor extending Auditable {
        required property name -> str {
          constraint max_len_value(50);
        }
        property age -> int16 {
          constraint min_value(0);
          constraint max_value(100);
        }
        property height -> int16 {
          constraint min_value(0);
          constraint max_value(300);
        }
      }

      type Movie extending Auditable {
        required property name -> str {
          constraint max_len_value(50);
        }
        property year -> int16{
          constraint min_value(1850);
        };
        multi link actors -> Actor;
      }
    }


Here, we've defined an ``abstract`` type called ``Auditable`` to take advantage
of EdgeDB's schema mixin system. This allows us to add a ``created_at``
property to multiple types without repeating ourselves.

The ``Actor`` type extends ``Auditable`` and inherits the ``created_at``
property as a result. This property is auto-filled via the ``datetime_current``
function. Along with the inherited type, the actor type also defines a few
additional properties like called ``name``, ``age``, and ``height``. The
constraints on the properties make sure that actor names can't be longer than
50 characters, age must be between 0 to 100 years, and finally, height must be
between 0 to 300 centimeters.

We also define a ``Movie`` type that extends the ``Auditable`` abstract type.
It also contains some additional concrete properties and links: ``name``,
``year``, and an optional multi-link called ``actors`` which refers to the
``Actor`` objects.

Build the API endpoints
=======================

The API endpoints are defined in the ``app`` directory. The directory structure
looks as follows:

::

    app
    ├── __init__.py
    ├── actors.py
    ├── main.py
    └── movies.py

The ``actors.py`` and ``movies.py`` modules contain the code to build the
``Actor`` and ``Movie`` APIs respectively. The ``main.py`` module then
registers all the endpoints and exposes them to the webserver.


Fetch actors
^^^^^^^^^^^^

Since the ``Actor`` type is simpler, we'll start with that. Let's
create a ``GET /actors`` endpoint so that we can see the ``Actor``
objects saved in the database. You can create the API in Flask like this:

.. code-block:: python

    # flask-crud/app/actors.py
    from __future__ import annotations

    import json
    from http import HTTPStatus

    import edgedb
    from flask import Blueprint, request

    actor = Blueprint("actor", __name__)
    client = edgedb.create_client()


    @actor.route("/actors", methods=["GET"])
    def get_actors() -> tuple[dict, int]:
        filter_name = request.args.get("filter_name")

        if not filter_name:
            actors = client.query_json(
                """
                select Actor {
                    name,
                    age,
                    height
                }
                """
            )
        else:
            actors = client.query_json(
                """
                select Actor {
                    name,
                    age,
                    height
                }
                filter .name = <str>$filter_name
                """,
                filter_name=filter_name,
            )

        response_payload = {"result": json.loads(actors)}
        return response_payload, HTTPStatus.OK


The ``Blueprint`` instance does the actual work of exposing the API. We also
create a blocking EdgeDB client instance to communicate with the database. By
default, this API will return a list of actors, but you can also filter the
objects by name.

In the ``get_actors`` function, we perform the database query via the
``edgedb`` client. Here, the ``client.query_json`` method conveniently returns
``JSON`` serialized objects. We deserialize the returned data in the
``response_payload`` dictionary and then return it. Afterward, the final JSON
serialization part is taken care of by Flask. This endpoint is exposed to the
server in the ``main.py`` module. Here's the content of the module:

.. code-block:: python

    # flask-crud/app/main.py
    from __future__ import annotations

    from flask import Flask

    from app.actors import actor
    from app.movies import movie

    app = Flask(__name__)

    app.register_blueprint(actor)
    app.register_blueprint(movie)


To test the endpoint, go to the ``flask-crud`` directory and run:

.. code-block:: bash

    $ export FLASK_APP=app.main:app && flask run --reload

This will start the development server and make it accessible via port 5000.
Earlier, we installed the `HTTPx <https://www.python-httpx.org/>`_ client
library to make HTTP requests programmatically. It also comes with a neat
command-line tool that we'll use to test our API.

While the development server is running, on a new console, run:

.. code-block:: bash

    $ httpx -m GET http://localhost:5000/actors

You'll see the following output on the console:

::

    HTTP/1.1 200 OK
    Server: Werkzeug/2.1.1 Python/3.10.4
    Date: Wed, 27 Apr 2022 18:58:38 GMT
    Content-Type: application/json
    Content-Length: 2

    {
      "result": []
    }

Our request yielded an empty list because the database is currently empty.
Let's create the ``POST /actors`` endpoint to start saving actors in the
database.

Create actor
^^^^^^^^^^^^

The POST endpoint can be built similarly:

.. code-block:: python

    # flask-crud/app/actors.py
    ...
    @actor.route("/actors", methods=["POST"])
    def post_actor() -> tuple[dict, int]:
        incoming_payload = request.json

        # Data validation.
        if not incoming_payload:
            return {
                "error": "Bad request"
            }, HTTPStatus.BAD_REQUEST

        if not (name := incoming_payload.get("name")):
            return {
                "error": "Field 'name' is required."
            }, HTTPStatus.BAD_REQUEST

        if len(name) > 50:
            return {
                "error": "Field 'name' cannot be longer than 50 "
                         "characters."
            }, HTTPStatus.BAD_REQUEST

        if age := incoming_payload.get("age"):
            if 0 <= age <= 100:
                return {
                    "error": "Field 'age' must be between 0 "
                    "and 100."
                }, HTTPStatus.BAD_REQUEST

        if height := incoming_payload.get("height"):
            if not 0 <= height <= 300:
                return {
                    "error": "Field 'height' must between 0 and "
                             "300 cm."
                }, HTTPStatus.BAD_REQUEST

        # Create object.
        actor = client.query_single_json(
            """
            with
                name := <str>$name,
                age := <optional int16>$age,
                height := <optional int16>$height
            select (
                insert Actor {
                    name := name,
                    age := age,
                    height := height
                }
            ){ name, age, height };
            """,
            name=name,
            age=age,
            height=height,
        )
        response_payload = {"result": json.loads(actor)}
        return response_payload, HTTPStatus.CREATED


In the above snippet, we perform data validation in the conditional blocks and
then make the query to create the object in the database. For now, we'll only
allow creating a single object per request. The ``client.query_single_json``
ensures that we're creating and returning only one object. Inside the query
string, notice, how we're using ``<optional type>`` to deal with the optional
fields. If the user doesn't provide the value of an optional field like ``age``
or ``height``, it'll be defaulted to ``null``.

To test it out, make a request as follows:

.. code-block:: bash

    $ httpx -m POST http://localhost:5000/actors \
            -j '{"name" : "Robert Downey Jr."}'

The output should look similar to this:

::

    HTTP/1.1 201 CREATED
    ...

    {
      "result": {
        "age": null,
        "height": null,
        "name": "Robert Downey Jr."
      }
    }


Before we move on to the next step, create 2 more actors called ``Chris Evans``
and ``Natalie Portman``. Now that we have some data in the database, let's
make a ``GET`` request to see the objects:

.. code-block:: bash

    $ httpx -m GET http://localhost:5000/actors

The response looks as follows:

::

    HTTP/1.1 200 OK
    ...

    {
      "result": [
        {
          "age": null,
          "height": null,
          "name": "Robert Downey Jr."
        },
        {
          "age": null,
          "height": null,
          "name": "Chris Evans"
        },
        {
          "age": null,
          "height": null,
          "name": "Natalie Portman"
        }
      ]
    }

You can filter the output of the ``GET /actors`` by ``name``. To do so, use the
``filter_name`` query parameter like this:

.. code-block:: bash

    $ httpx -m GET http://localhost:5000/actors \
            -p filter_name "Robert Downey Jr."

Doing this will only display the data of a single object:

::

    HTTP/1.1 200 OK

    {
      "result": [
        {
          "age": null,
          "height": null,
          "name": "Robert Downey Jr."
        }
      ]
    }

Once you've done that, we can move on to the next step of building the
``PUT /actors`` endpoint to update the actor data.


Update actor
^^^^^^^^^^^^

It can be built like this:


.. code-block:: python

    # flask-crud/app/actors.py

    # ...

    @actor.route("/actors", methods=["PUT"])
    def put_actors() -> tuple[dict, int]:
        incoming_payload = request.json
        filter_name = request.args.get("filter_name")

        # Data validation.
        if not incoming_payload:
            return {
                "error": "Bad request"
            }, HTTPStatus.BAD_REQUEST

        if not filter_name:
            return {
                "error": "Query parameter 'filter_name' must "
                "be provided",
            }, HTTPStatus.BAD_REQUEST

        if (name:=incoming_payload.get("name")) and len(name) > 50:
            return {
                "error": "Field 'name' cannot be longer than "
                "50 characters."
            }, HTTPStatus.BAD_REQUEST

        if age := incoming_payload.get("age"):
            if age <= 0:
                return {
                    "error": "Field 'age' cannot be less than "
                    "or equal to 0."
                }, HTTPStatus.BAD_REQUEST

        if height := incoming_payload.get("height"):
            if not 0 <= height <= 300:
                return {
                    "error": "Field 'height' must between 0 "
                    "and 300 cm."
                }, HTTPStatus.BAD_REQUEST

        # Update object.
        actors = client.query_json(
            """
            with
                filter_name := <str>$filter_name,
                name := <optional str>$name,
                age := <optional int16>$age,
                height := <optional int16>$height
            select (
                update Actor
                filter .name = filter_name
                set {
                    name := name ?? .name,
                    age := age ?? .age,
                    height := height ?? .height
                }
            ){ name, age, height };""",
            filter_name=filter_name,
            name=name,
            age=age,
            height=height,
        )
        response_payload = {"result": json.loads(actors)}
        return response_payload, HTTPStatus.OK

Here, we'll isolate the intended object that we want to update by filtering the
actors with the ``filter_name`` parameter. For example, if you wanted to update
the properties of ``Robert Downey Jr.``, the value of the ``filter_name``
query parameter would be ``Robert Downey Jr.``. The coalesce operator ``??``
in the query string makes sure that the API user can selectively update the
properties of the target object and the other properties keep their existing
values.

The following command updates the ``age`` and ``height`` of
``Robert Downey Jr.``.

.. code-block:: bash

    $ httpx -m PUT http://localhost:5000/actors \
            -p filter_name "Robert Downey Jr." \
            -j '{"age": 57, "height": 173}'

This will return:

::

    HTTP/1.1 200 OK
    ...
    {
      "result": [
        {
          "age": 57,
          "height": 173,
          "name": "Robert Downey Jr."
        }
      ]
    }


Delete actor
^^^^^^^^^^^^

Another API that we'll need to cover is the ``DELETE /actors`` endpoint. It'll
allow us to query the name of the targeted object and delete that. The code
looks similar to the ones you've already seen:

.. code-block:: python

    # flask-crud/app/actors.py
    ...

    @actor.route("/actors", methods=["DELETE"])
    def delete_actors() -> tuple[dict, int]:
        if not (filter_name := request.args.get("filter_name")):
            return {
                "error": "Query parameter 'filter_name' must "
                "be provided",
            }, HTTPStatus.BAD_REQUEST

        try:
            actors = client.query_json(
                """select (
                    delete Actor
                    filter .name = <str>$filter_name
                ) {name}
                """,
                filter_name=filter_name,
            )
        except edgedb.errors.ConstraintViolationError:
            return (
                {
                    "error": f"Cannot delete '{filter_name}. "
                    "Actor is associated with at least one movie."
                },
                HTTPStatus.BAD_REQUEST,
            )

        response_payload = {"result": json.loads(actors)}
        return response_payload, HTTPStatus.OK


This endpoint will simply delete the requested actor if the actor isn't
attached to any movie. If the targeted object is attached to a movie, then API
will throw an HTTP 400 (bad request) error and refuse to delete the object. To
delete ``Natalie Portman``, on your console, run:

.. code-block:: bash

    $ httpx -m DELETE http://localhost:5000/actors \
            -p filter_name "Natalie Portman"

That'll return:

::

    HTTP/1.1 200 OK
    ...

    {
      "result": [
        {
          "name": "Natalie Portman"
        }
      ]
    }


Now let's move on to building the ``Movie`` API.

Create movie
^^^^^^^^^^^^

Here's how we'll implement the ``POST /movie`` endpoint:

.. code-block:: python

    # flask-crud/app/movies.py
    from __future__ import annotations

    import json
    from http import HTTPStatus

    import edgedb
    from flask import Blueprint, request

    movie = Blueprint("movie", __name__)
    client = edgedb.create_client()

    @movie.route("/movies", methods=["POST"])
    def post_movie() -> tuple[dict, int]:
        incoming_payload = request.json

        # Data validation.
        if not incoming_payload:
            return {
                "error": "Bad request"
            }, HTTPStatus.BAD_REQUEST

        if not (name := incoming_payload.get("name")):
            return {
                "error": "Field 'name' is required."
            }, HTTPStatus.BAD_REQUEST

        if len(name) > 50:
            return {
                "error": "Field 'name' cannot be longer than "
                "50 characters."
            }, HTTPStatus.BAD_REQUEST

        if year := incoming_payload.get("year"):
            if year < 1850:
                return {
                    "error": "Field 'year' cannot be less "
                    "than 1850."
                }, HTTPStatus.BAD_REQUEST

        actor_names = incoming_payload.get("actor_names")

        # Create object.
        movie = client.query_single_json(
            """
            with
                name := <str>$name,
                year := <optional int16>$year,
                actor_names := <optional array<str>>$actor_names
            select (
                insert Movie {
                    name := name,
                    year := year,
                    actors := (
                        select Actor
                        filter .name in array_unpack(actor_names)
                    )
                }
            ){ name, year, actors: {name, age, height} };
            """,
            name=name,
            year=year,
            actor_names=actor_names,
        )
        response_payload = {"result": json.loads(movie)}
        return response_payload, HTTPStatus.CREATED

Like the ``POST /actors`` API, conditional blocks validate the shape of the
incoming data and the ``client.query_json`` method creates the object in the
database. EdgeQL allows us to perform insertion and selection of data fields
at the same time in a single query. One thing that's different here is that the
``POST /movies`` API also accepts an optional field called ``actor_names``
where the user can provide an array of actor names. The backend will associate
the actors with the movie object if those actors exist in the database.

Here's how you'd create a movie:


.. lint-off

.. code-block:: bash

    $ httpx -m POST http://localhost:5000/movies \
            -j '{ "name": "The Avengers", "year": 2012, "actor_names": [ "Robert Downey Jr.", "Chris Evans" ] }'

.. lint-on

That'll return:

::

    HTTP/1.1 201 CREATED
    ...
    {
      "result": {
        "actors": [
          {
            "age": null,
            "height": null,
            "name": "Chris Evans"
          },
          {
            "age": 57,
            "height": 173,
            "name": "Robert Downey Jr."
          }
        ],
        "name": "The Avengers",
        "year": 2012
      }
    }

Additional movie endpoints
^^^^^^^^^^^^^^^^^^^^^^^^^^

The implementation of the ``GET /movie``, ``PATCH /movie`` and
``DELETE /movie`` endpoints are provided in the sample codebase in
``app/movies.py``. But try to write them on your own using the Actor endpoints
as a starting point! Once you're down, you should be able to fetch a movie by
it's title from your database by  the ``filter_name`` parameter with
the GET API as follows:

.. code-block:: bash

    $ httpx -m GET http://localhost:5000/movies \
            -p 'filter_name' 'The Avengers'

That'll return:

::

    HTTP/1.1 200 OK
    ...
    {
      "result": [
        {
          "actors": [
            {
              "age": null,
              "name": "Chris Evans"
            },
            {
              "age": 57,
              "name": "Robert Downey Jr."
            }
          ],
          "name": "The Avengers",
          "year": 2012
        }
      ]
    }



Conclusion
==========

While builing REST APIs, the EdgeDB client allows you to leverage EdgeDB with
any microframework of your choice. Whether it's
`FastAPI <https://fastapi.tiangolo.com>`_,
`Flask <https://flask.palletsprojects.com>`_,
`AIOHTTP <https://docs.aiohttp.org/en/stable>`_,
`Starlette <https://www.starlette.io>`_,
or `Tornado <https://www.tornadoweb.org/en/stable>`_,
the core workflow is quite similar to the one demonstrated above; you'll query
and serialize data with the client and then return the payload for your
framework to process.
