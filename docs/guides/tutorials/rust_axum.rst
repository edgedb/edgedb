=========
Rust Axum
=========

This guide will show you step by step how to create a small app in Rust
that uses Axum as its web server and EdgeDB to hold weather data. The app 
itself simply calls into the Open-Meteo API to look for updated weather
information on the cities in the database, and goes back to sleep for a
minute every time it finishes looking for updates.

Open-Meteo is being used here because
`their service <https://open-meteo.com/en/docs>`_ doesn't require any sort
of registration or API key. Give it a try `in your browser`_! We'll be
saving the time and temperature information from this output to the database.

Getting started
---------------

To get started, first create a new Cargo project with
``cargo new weather_app``, or whatever name you would like to call it.
Go into the directory that was created and type ``edgedb project init``
to start an EdgeDB instance. Inside, you will see your schema inside
the ``default.esdl`` in the ``/dbschema`` directory.

Schema
------

The schema inside ``default.esdl`` is simple but leverages a lot of EdgeDB's
guarantees so that we don't have to think about them on the client side. Here
is what the final schema will look like:

.. code-block:: sdl

  module default {

    scalar type Latitude extending float64 {
      constraint max_value(90.0);
      constraint min_value(-90.0);
    }

    scalar type Longitude extending float64 {
      constraint max_value(180.0);
      constraint min_value(-180.0);
    }

    scalar type Temperature extending float64 {
      constraint max_value(70.0);
      constraint min_value(-100.0);
    }

    type City {
      required name: str {
        constraint exclusive;
      }
      required latitude: Latitude;
      required longitude: Longitude;
      multi conditions := (select .<city[is Conditions] order by .time);
    }

    type Conditions {
      required city: City {
        on target delete delete source;
      }
      required temperature: Temperature;
      required time: str;
      constraint exclusive on ((.time, .city));
    }
  }

Let's go over it one part at a time to see what advantages EdgeDB has
given us even in a schema as simple as this one.

First are the three scalar types extend ``float64`` to give us some type
safety when it comes to latitude, longitude, and temperature. Latitude can't
exceed 90 degrees, and longitude can't exceed 180. Open-Meteo does its own
checks for latitude and longitude when querying the conditions for a location,
but we might decide to switch to a different weather service one day
and having constraints in our schema up front makes it easy for users to see
which values are valid and which are not.

On top of this, sometimes another server's data will just go haywire for some
reason or another, such as the time a weather map showed a high of 
`thousands of degrees <https://www.youtube.com/watch?v=iXuc7SAyk2s>`_ (!) for
various cities in Arizona. If our database simply accepted any and all output,
we might end up with some weird outlying numbers that affect any calculations
we make on the data. With the constraints in place, we are at least guaranteed
to not add temperature data that reaches that point! The highest and lowest
temperatures ever recorded on Earth are 56.7 °C and -89.2°C, so 70.0 and
-100.00 should be a good range for our ``Temperature`` scalar type.

.. code-block:: sdl

    scalar type Latitude extending float64 {
        constraint max_value(90.0);
        constraint min_value(-90.0);
    }

    scalar type Longitude extending float64 {
        constraint max_value(180.0);
        constraint min_value(-180.0);
    }

    scalar type Temperature extending float64 {
        constraint max_value(70.0);
        constraint min_value(-100.0);
    }

Open-Meteo returns a good deal of information when you query it for current
weather. The endpoint in the link above produces an output that looks like
this:

.. code-block::

    {
        "latitude": 49.9375,
        "longitude": 50,
        "generationtime_ms": 0.06699562072753906,
        "utc_offset_seconds": 3600,
        "timezone": "Europe/Paris",
        "timezone_abbreviation": "CET",
        "elevation": 6,
        "current_weather_units": {
            "time": "iso8601",
            "interval": "seconds",
            "temperature": "°C",
            "windspeed": "km/h",
            "winddirection": "°",
            "is_day": "",
            "weathercode": "wmo code"
        },
        "current_weather": {
            "time": "2024-02-05T04:00",
            "interval": 900,
            "temperature": 0.6,
            "windspeed": 20.2,
            "winddirection": 231,
            "is_day": 0,
            "weathercode": 3
        }
    }

To keep the weather app simple, we will only use ``time`` and ``temperature``
located inside ``current_weather``. (Small challenge: feel free to grow the
schema with other scalar types to incorporate all the other information
returned from Open-Meteo!)

We can then use this info to insert a type called ``Conditions`` that
will look like this:

.. code-block:: sdl

    type Conditions {
        required city: City {
            on target delete delete source;
        }
        required temperature: Temperature;
        required time: str;
        constraint exclusive on ((.time, .city));
    }

EdgeDB's deletion policies are a nice plus here. Users are allowed to delete
cities from the database, but just deleting a ``City`` object would leave a
lot of ``Conditions`` types floating around (and which are ``required`` to be
linked to a ``City`` in any case). So we can use ``on target delete delete
source`` so that any time a ``City`` object is deleted, all of the now useless
``Conditions`` objects get deleted along with it.

This type also contains an ``exclusive`` constraint on time and city, because
the app will continue to query Open-Meteo once a minute for data but shouldn't
insert a ``Conditions`` object for a city and time that has already been
inserted. In Open-Meteo's case, these weather conditions are updated every
15 minutes, so we will end up seeing four temperatures an hour added for
each city.

The ``City`` type is pretty simple:

.. code-block:: sdl

    type City {
    required name: str {
        constraint exclusive;
      }
    required latitude: Latitude;
    required longitude: Longitude;
    multi conditions := (select .<city[is Conditions] order by .time);
    }

The line with
``multi conditions := (select .<city[is Conditions] order by .time);``
is a backlink, giving us access to any ``Conditions`` objects connected to
a ``City`` by a link called ``city``. A backlink alone would look like
this: ``.<city[is Conditions]``. But we might as well order the ``conditions``
by date here so that we don't have to do it inside the Rust code, or any
other programming language we might want to use. Here, EdgeDB ensures that our
output is consistent regardless of which programming language we use to build
an app using this data.

``City`` has an ``exclusive`` constraint for city names. This is fine for our
simple app, but in reality we would want to change this because a city can
have the same name as another. One possibility later on would be to give a
``City`` a computed key formed from the ``name``, ``latitude``, and
``longitude``. Then ``latitude`` and ``longitude`` could be cast into an
``int64`` before being cast into a ``str`` so that users could not insert
a city of the same name that is only a fraction of a degree different from
an existing city (i.e. the same location).

.. code-block:: sdl-diff

  type City {
    required name: str;
    required latitude: Latitude;
    required longitude: Longitude;
    multi conditions := (select .<city[is Conditions] order by .time);
  + key := .name ++ <str><int64>.latitude ++ <str><int64>.longitude;
  + constraint exclusive on (.key);
  }

You could give this or another method a try if you are feeling ambitious.

And with that out of the way, let's move on to the Rust code.

Rust code
---------

Here are the dependencies you will need to add to ``cargo.toml`` (with
the exception of ``anyhow`` which isn't strictly needed but is always
nice to use).

.. code-block::

  anyhow = "1.0.79"
  axum = "0.7.4"
  edgedb-errors = "0.4.1"
  edgedb-protocol = "0.6.0"
  edgedb-tokio = "0.5"
  reqwest = "0.11.24"
  serde = "1.0.196"
  serde_json = "1.0.113"
  tokio = { version = "1.36.0", features = ["rt", "macros"] }

And then a few use statements at the top:

.. code-block::

    use axum::{
        extract::{Path, State},
        routing::get,
        Router,
    };

    use edgedb_errors::ConstraintViolationError;
    use edgedb_protocol::value::Value;
    use edgedb_tokio::{create_client, Client, Queryable};
    use serde::Deserialize;
    use std::time::Duration;
    use tokio::{time::sleep, net::TcpListener};

And now to the real code.

The first part of the code is just a few functions that return a ``String`` or
a ``&'static str``. They aren't strictly necessary, but are nice to have on
so that we can review all the queries we will need in one place and keep the
following code clean. Note that the ``select_city()`` function also has an
optional filter, and uses a ``mut String`` instead of the ``format!`` macro
because inside ``format!`` you need to use the ``{{`` double brace escape
sequence in place of single braces, which quickly makes things ugly.

.. code-block:: rust

  fn select_city(filter: &str) -> String {
    let mut output = "select City { 
      name, 
      latitude, 
      longitude,
      conditions: { temperature, time }
    } "
    .to_string();
    output.push_str(filter);
    output
  }

  fn insert_city() -> &'static str {
    "insert City {
      name := <str>$0,
      latitude := <float64>$1,
      longitude := <float64>$2,
    };"
  }

  fn insert_conditions() -> &'static str {
    "insert Conditions {
      city := (select City filter .name = <str>$0),
      temperature := <float64>$1,
      time := <str>$2 
    }"
  }

  fn delete_city() -> &'static str {
    "delete City filter .name = <str>$0"
  }

  fn select_city_names() -> &'static str {
    "select City.name order by City.name"
  }

Next are a few structs to work with the output from Open-Meteo, and a
function that uses ``reqwest`` to get the weather information we need and
deserialize it into a Rust type.

.. code-block:: rust

  #[derive(Queryable)]
  struct City {
    name: String,
    latitude: f64,
    longitude: f64,
    conditions: Option<Vec<CurrentWeather>>,
  }

  #[derive(Deserialize, Queryable)]
  struct WeatherResult {
    current_weather: CurrentWeather,
  }

  #[derive(Deserialize, Queryable)]
  struct CurrentWeather {
    temperature: f64,
    time: String,
  }

  async fn weather_for(latitude: f64, longitude: f64) -> 
        Result<CurrentWeather, anyhow::Error> 
  {
    let url = format!("https://api.open-meteo.com/v1/forecast?\
      latitude={latitude}&longitude={longitude}\
      &current_weather=true&timezone=CET");
    let res = reqwest::get(url).await?.text().await?;
    let weather_result: WeatherResult = serde_json::from_str(&res)?;
    Ok(weather_result.current_weather)
  }

Next up is the app itself! It's called a ``WeatherApp`` and simply holds the
Client to connect to EdgeDB.

.. code-block:: rust

  struct WeatherApp {
    db: Client,
  }

Then inside ``impl WeatherApp`` we have a few methods.

First there is ``init()``, which just gives the app some initial data. We'll
choose the small country of Andorra located in between Spain and France and
where the Catalan language is spoken. With a country of that size we can
insert just six cities and have full coverage of its nationwide weather
conditions. Note that the ``Error`` type for the EdgeDB client has an
``.is()`` method that lets us check what sort of error was returned. We will
use it to check for a ``ConstraintViolationError`` to see if a city has
already been inserted, and otherwise print an "Unexpected error" for anything
else.

.. code-block:: rust

  async fn init(&self) {
    let city_data = [
      ("Andorra la Vella", 42.3, 1.3),
      ("El Serrat", 42.37, 1.33),
      ("Encamp", 42.32, 1.35),
      ("Les Escaldes", 42.3, 1.32),
      ("Sant Julià de Lòria", 42.28, 1.29),
      ("Soldeu", 42.34, 1.4),
    ];

    let query = insert_city();
      for (name, lat, long) in city_data {
        match self.db.execute(query, &(name, lat, long)).await {
          Ok(_) => println!("City {name} inserted!"),
          Err(e) => {
            if e.is::<ConstraintViolationError>() {
                println!("City {name} already in db");
            } else {
                println!("Unexpected error: {e:?}");
            }
          }
        }
      }
    }

The ``.get_cities()`` method simply returns all the cities in the database
without filtering. The ``.update_conditions()`` method then uses this
to cycle through the cities and get their weather conditions. The
``Conditions`` type in our database has a
``constraint exclusive on ((.time, .city));``. Most of the time the
results from Open-Meteo will violate this and a new object will not be
inserted, and so inside ``update_conditions`` we won't log anything if
this is the case as this is expected behavior. In practice, we know that
new conditions will only be added every 15 minutes, but there is no
guarantee what Open-Meteo's future behavior might be, or if our weather
app will start using another service or multiple services to get weather
info, so the easiest thing to do is just keep looping while ignoring
constraint violation errors. All we are concerned with is keeping weather
information that does has a new time stamp, and ignoring the rest.

.. code-block:: rust

  async fn get_cities(&self) -> Result<Vec<City>, anyhow::Error> {
    Ok(self.db.query::<City, _>(&select_city(""), &()).await?)
  }

  async fn update_conditions(&self) -> Result<(), anyhow::Error> {
    for City {
      name,
      latitude,
      longitude,
      .. 
        } in self.get_cities().await?
      {
        let CurrentWeather { temperature, time } = 
            weather_for(latitude, longitude).await?;

        match self
          .db
          .execute(insert_conditions(), &(&name, temperature, time))
          .await
        {
          Ok(()) => println!("Inserted new conditions for {}", name),
          Err(e) => {
            if !e.is::<ConstraintViolationError>() {
              println!("Unexpected error: {e}");
              }
            }
          }
        }
    Ok(())
    }

Finally, a ``.run()`` method will get our ``WeatherApp`` to run forever,
sleeping for 60 seconds each time.

.. code-block:: rust

  async fn run(&self) {
    loop {
      println!("Looping...");
      if let Err(e) = self.update_conditions().await {
        println!("Loop isn't working: {e}")
        }
      sleep(Duration::from_secs(60)).await;
        }
      }
    }

So that code will be enough to have an app that loops forever, looking for
new weather information. But we'd also like users to be able to add and
remove cities, and Axum will allow us to add some endpoints to make this
happen. To start, we'll put a ``menu()`` function together that simply
lists the endpoints so that the user knows what options are available when
they access ``http://localhost:3000/``. Note that the function is an
``async fn`` despite not having any async code because Axum requires all
routes to be handled by an async function (or closure).

.. code-block:: rust

  async fn menu() -> &'static str {
      "Routes:
        /conditions/<name>
        /add_city/<name>/<latitude>/<longitude>
        /remove_city/<name>
        /city_names"
  }

So our API will allow users to see the conditions for a city, to add a city
along with its location, remove a city, and also display a list of all city
names in the database.

Before we get to the functions for each endpoint, we should take a look at
``main()`` to get an idea of what everything will look like. We will first
create a ``Client`` to the database, and add it as a parameter inside the
``WeatherApp``. Cloning an EdgeDB Client is cheap and easy to do, so we will
do this and then add the ``Client`` to Axum's ``.with_state()`` method, which
will make it available inside the Axum endpoint functions whenever we need it.
Meanwhile, the ``WeatherApp`` will simply ``.run()`` forever inside its own
tokio task.

All together, the code for ``main()`` looks like this:

.. code-block:: rust

  #[tokio::main]
  async fn main() -> Result<(), anyhow::Error> {
    let client = create_client().await?;

    let weather_app = WeatherApp { db: client.clone() };

    weather_app.init().await;

    tokio::task::spawn(async move {
      weather_app.run().await;
    });

    let app = Router::new()
      .route("/", get(menu))
      .route("/conditions/:name", get(get_conditions))
      .route("/add_city/:name/:latitude/:longitude", get(add_city))
      .route("/remove_city/:name", get(remove_city))
      .route("/city_names", get(city_names))
      .with_state(client)
      .into_make_service();

    let listener = TcpListener::bind("0.0.0.0:3000").await.unwrap();
    axum::serve(listener, app).await.unwrap();
    Ok(())
    }

Now we just need to write the Axum functions to complete our app.

Removing a City is pretty easy: just use this query returned by the
``delete_city()`` function and call ``.query()`` with it.

.. code-block::

  "delete City filter .name = <str>$0"

We don't need to deserialize the result, and instead can just return a
``Vec<Value>`` and check to see if it's empty or not. If it's empty,
then no city matched the name we specified.

Also note the destructuring inside function signatures here, which is pretty
convenient! Axum makes use of this pattern in its examples quite a bit.

.. code-block:: rust

  async fn remove_city(Path(name): Path<String>, State(client): State<Client>)
       -> String 
    {
    match client
      .query::<Value, _>(delete_city(), &(&name,))
      .await
    {
      Ok(v) if v.is_empty() => format!("No city {name} found to remove!"),
      Ok(_) => format!("City {name} removed!"),
      Err(e) => e.to_string(),
    }
  }

Getting a list of city names is just as easy. The query is just a few word
long:

.. code-block::

  "select City.name order by City.name"

And so is the method to do the query. It will just return a set of strings,
so we don't need to deserialize it into our own Rust type either:

.. code-block:: rust

    async fn city_names(State(client): State<Client>) -> String {
        match client
            .query::<String, _>(select_city_names(), &())
            .await
        {
            Ok(cities) => format!("{cities:#?}"),
            Err(e) => e.to_string(),
        }
    }

The next function is ``get_conditions()``, which users will make the most 
use of. The query is a simple ``select``:

.. code-block::

  "select City { 
    name, 
    latitude, 
    longitude,
    conditions: { temperature, time }
  } "

After which we will filter on the name of the ``City``. The method used here
is ``.query_required_single()``, because we know that only a single ``City``
can be returned thanks to the ``exclusive`` constraint on its ``name``
property. Don't forget that our ``City`` objects already order their weather
conditions by time, so we don't need to do any ordering ourselves:

.. code-block::

  multi conditions := (select .<city[is Conditions] order by .time);

Once a ``City`` object is returned, we'll just format the output a little to
make it nicer. A ``datetime`` in EdgeDB always has a ``T`` separator between
the date and the time, so we can use the ``.split_once()`` method to split
it into two and thereby get rid of the ``T``.

.. code-block:: rust

  async fn get_conditions(Path(city_name): Path<String>, 
      State(client): State<Client>) -> String 
    {
    let query = select_city("filter .name = <str>$0");
    match client
      .query_required_single::<City, _>(&query, &(&city_name,))
      .await
    {
      Ok(city) => {
        let mut conditions = format!("Conditions for {city_name}:\n\n");
        for condition in city.conditions.unwrap_or_default() {
          let (date, hour) = condition.time.split_once("T")
            .unwrap_or_default();
          conditions.push_str(&format!("{date} {hour}\t"));
          conditions.push_str(&format!("{}\n", condition.temperature));
        }
        conditions
        }
      Err(e) => format!("Couldn't find {city_name}: {e}"),
      }
  }

Adding a ``City`` is a tiny bit more complicated, because we don't know
exactly how Open-Meteo's internals work. That means that there is always
a chance that a request might not work for some reason, and in that case
we don't want to insert a ``City`` into our database because then the
``WeatherApp`` will just keep requesting data that Open-Meteo refuses
to provide.

In fact, you can take a look at this by trying a query for Open-Meteo for
a location at latitude 80.0 or longitude 180.0. They won't work, because
Open-Meteo allows queries *up to or less than* these values, but in our
database we allow these values to be *up to* 80.0 and 180.0. Our example
code pretends that we didn't notice that. Plus, there is no guarantee that
Open-Meteo will be the only service that our weather app uses, and other
services might allow values of 80.0 and 180.0.

So that means that the ``add_city()`` function will first make sure that
Open-Meteo returns a good result, and only then inserts a ``City``. With this
step done, it will insert the most recent conditions for the new city using
the ``.execute()`` method which returns ``()``. These two steps could also be
done in a single query in EdgeDB, but doing it one simple step at a time feels
most readable here and allows us to see at which point an error happens if
that is the case.

.. code-block:: rust

  async fn add_city(
    State(client): State<Client>,
    Path((name, lat, long)): Path<(String, f64, f64)>,
  ) -> String {
    // First make sure that Open-Meteo is okay with it
    let (temperature, time) = match weather_for(lat, long).await {
      Ok(c) => (c.temperature, c.time),
      Err(e) => {
        return format!("Couldn't get weather info: {e}");
      }
    };

    // Then insert the City
    if let Err(e) = client.execute(insert_city(), &(&name, lat, long)).await {
      return e.to_string();
    }

    // And finally the Conditions
    if let Err(e) = client
      .execute(insert_conditions(), &(&name, temperature, time))
      .await
    {
      return format!("Inserted City {name} \
        but couldn't insert conditions: {e}");
    }
  format!("Inserted city {name}!")
  }

And with that, we have our app! Running the app inside the console should
produce the following output, with extra lines for any cities you add
yourself.

.. code-block::

    Inserted new conditions for Andorra la Vella
    Inserted new conditions for Encamp
    Inserted new conditions for Les Escaldes
    Inserted new conditions for Sant Julià de Lòria
    Inserted new conditions for Soldeu
    Inserted new conditions for El Serrat
    Looping...
    Looping...
    Looping...

And inside your browser you should be able to see any city you like with
an address like the following: ``http://localhost:3000/conditions/El Serrat``
The output will look like this:

.. code-block::

    Conditions for El Serrat:

    2024-02-05 01:30	4.5
    2024-02-05 02:15	4.6
    2024-02-05 02:30	4.5
    2024-02-05 02:45	4.7
    ... and so on...

So that's how to get started with EdgeDB and Axum! You can now use this code
as a template to modify to get your own app started. Rust's other main web
servers are implemented with Actix-web and Rocket, and modifying the code
to fit them is not all that hard. Changing the code below into code that works
for Actix-web or Rocket could be a good exercise to internalize it in your
mind if you are still new to EdgeDB and/or Rust and want some active practice.

Here is all of the Rust code:

.. lint-off

.. code-block:: rust

  use axum::{
      extract::{Path, State},
      routing::get,
      Router,
  };

  use edgedb_errors::ConstraintViolationError;
  use edgedb_protocol::value::Value;
  use edgedb_tokio::{create_client, Client, Queryable};
  use serde::Deserialize;
  use std::time::Duration;
  use tokio::{net::TcpListener, time::sleep};

  fn select_city(filter: &str) -> String {
      let mut output = "select City { 
          name, 
          latitude, 
          longitude,
          conditions: { temperature, time }
      } "
      .to_string();
      output.push_str(filter);
      output
  }

  fn insert_city() -> &'static str {
      "insert City {
          name := <str>$0,
          latitude := <float64>$1,
          longitude := <float64>$2,
      };"
  }

  fn insert_conditions() -> &'static str {
      "insert Conditions {
          city := (select City filter .name = <str>$0),
          temperature := <float64>$1,
          time := <str>$2 
      }"
  }

  fn delete_city() -> &'static str {
      "delete City filter .name = <str>$0"
  }

  fn select_city_names() -> &'static str {
      "select City.name order by City.name"
  }

  #[derive(Queryable)]
  struct City {
      name: String,
      latitude: f64,
      longitude: f64,
      conditions: Option<Vec<CurrentWeather>>,
  }

  #[derive(Deserialize, Queryable)]
  struct WeatherResult {
      current_weather: CurrentWeather,
  }

  #[derive(Deserialize, Queryable)]
  struct CurrentWeather {
      temperature: f64,
      time: String,
  }

  async fn weather_for(latitude: f64, longitude: f64) -> Result<CurrentWeather, anyhow::Error> {
      let url = format!(
          "https://api.open-meteo.com/v1/forecast?\
          latitude={latitude}&longitude={longitude}\
          &current_weather=true&timezone=CET"
      );
      let res = reqwest::get(url).await?.text().await?;
      let weather_result: WeatherResult = serde_json::from_str(&res)?;
      Ok(weather_result.current_weather)
  }

  struct WeatherApp {
      db: Client,
  }

  impl WeatherApp {
      async fn init(&self) {
          let city_data = [
              ("Andorra la Vella", 42.3, 1.3),
              ("El Serrat", 42.37, 1.33),
              ("Encamp", 42.32, 1.35),
              ("Les Escaldes", 42.3, 1.32),
              ("Sant Julià de Lòria", 42.28, 1.29),
              ("Soldeu", 42.34, 1.4),
          ];

          let query = insert_city();
          for (name, lat, long) in city_data {
              match self.db.execute(query, &(name, lat, long)).await {
                  Ok(_) => println!("City {name} inserted!"),
                  Err(e) => {
                      if e.is::<ConstraintViolationError>() {
                          println!("City {name} already in db");
                      } else {
                          println!("Unexpected error: {e:?}");
                      }
                  }
              }
          }
      }

      async fn get_cities(&self) -> Result<Vec<City>, anyhow::Error> {
          Ok(self.db.query::<City, _>(&select_city(""), &()).await?)
      }

      async fn update_conditions(&self) -> Result<(), anyhow::Error> {
          for City {
              name,
              latitude,
              longitude,
              ..
          } in self.get_cities().await?
          {
              let CurrentWeather { temperature, time } = weather_for(latitude, longitude).await?;

              match self
                  .db
                  .execute(insert_conditions(), &(&name, temperature, time))
                  .await
              {
                  Ok(()) => println!("Inserted new conditions for {}", name),
                  Err(e) => {
                      if !e.is::<ConstraintViolationError>() {
                          println!("Unexpected error: {e}");
                      }
                  }
              }
          }
          Ok(())
      }

      async fn run(&self) {
          sleep(Duration::from_millis(100)).await;
          loop {
              println!("Looping...");
              if let Err(e) = self.update_conditions().await {
                  println!("Loop isn't working: {e}")
              }
              sleep(Duration::from_secs(60)).await;
          }
      }
  }

  // Axum functions

  async fn menu() -> &'static str {
      "Routes:
              /conditions/<name>
              /add_city/<name>/<latitude>/<longitude>
              /remove_city/<name>
              /city_names"
  }

  async fn get_conditions(Path(city_name): Path<String>, State(client): State<Client>) -> String {
      let query = select_city("filter .name = <str>$0");
      match client
          .query_required_single::<City, _>(&query, &(&city_name,))
          .await
      {
          Ok(city) => {
              let mut conditions = format!("Conditions for {city_name}:\n\n");
              for condition in city.conditions.unwrap_or_default() {
                  let (date, hour) = condition.time.split_once("T").unwrap_or_default();
                  conditions.push_str(&format!("{date} {hour}\t"));
                  conditions.push_str(&format!("{}\n", condition.temperature));
              }
              conditions
          }
          Err(e) => format!("Couldn't find {city_name}: {e}"),
      }
  }

  async fn add_city(
      State(client): State<Client>,
      Path((name, lat, long)): Path<(String, f64, f64)>,
  ) -> String {
      // First make sure that Open-Meteo is okay with it
      let (temperature, time) = match weather_for(lat, long).await {
          Ok(c) => (c.temperature, c.time),
          Err(e) => {
              return format!("Couldn't get weather info: {e}");
          }
      };

      // Then insert the City
      if let Err(e) = client.execute(insert_city(), &(&name, lat, long)).await {
          return e.to_string();
      }

      // And finally the Conditions
      if let Err(e) = client
          .execute(insert_conditions(), &(&name, temperature, time))
          .await
      {
          return format!("Inserted City {name} but couldn't insert conditions: {e}");
      }

      format!("Inserted city {name}!")
  }

  async fn remove_city(Path(name): Path<String>, State(client): State<Client>) -> String {
      match client.query::<Value, _>(delete_city(), &(&name,)).await {
          Ok(v) if v.is_empty() => format!("No city {name} found to remove!"),
          Ok(_) => format!("City {name} removed!"),
          Err(e) => e.to_string(),
      }
  }

  async fn city_names(State(client): State<Client>) -> String {
      match client.query::<String, _>(select_city_names(), &()).await {
          Ok(cities) => format!("{cities:#?}"),
          Err(e) => e.to_string(),
      }
  }

  #[tokio::main]
  async fn main() -> Result<(), anyhow::Error> {
      let client = create_client().await?;

      let weather_app = WeatherApp { db: client.clone() };

      weather_app.init().await;

      tokio::task::spawn(async move {
          weather_app.run().await;
      });

      let app = Router::new()
          .route("/", get(menu))
          .route("/conditions/:name", get(get_conditions))
          .route("/add_city/:name/:latitude/:longitude", get(add_city))
          .route("/remove_city/:name", get(remove_city))
          .route("/city_names", get(city_names))
          .with_state(client)
          .into_make_service();

      let listener = TcpListener::bind("0.0.0.0:3000").await.unwrap();
      axum::serve(listener, app).await.unwrap();
      Ok(())
  }

.. _in your browser: https://api.open-meteo.com/v1/forecast?latitude=37&longitude=126&current_weather=true&timezone=CET

.. lint-on

Let's finish up this guide with two quick tips on how to speed up your
development time when working with JSON, Rust types, and EdgeQL queries.

Generating structs from JSON and queries from structs
-----------------------------------------------------

EdgeDB's Rust client does not yet have a query builder, but there are some
ways to speed up some of the manual typing you often need to do to ensure
type safety in Rust.

Let's say you wanted to put together some structs to incorporate more of this
output from the Open-Meteo endpoint that we have been using:

.. code-block::

  {
      "latitude": 49.9375,
      "longitude": 50,
      "generationtime_ms": 0.06604194641113281,
      "utc_offset_seconds": 3600,
      "timezone": "Europe/Paris",
      "timezone_abbreviation": "CET",
      "elevation": 6,
      "current_weather_units": {
          "time": "iso8601",
          "interval": "seconds",
          "temperature": "°C",
          "windspeed": "km/h",
          "winddirection": "°",
          "is_day": "",
          "weathercode": "wmo code"
      },
      "current_weather": {
          "time": "2024-02-07T01:00",
          "interval": 900,
          "temperature": -3.7,
          "windspeed": 38.9,
          "winddirection": 289,
          "is_day": 0,
          "weathercode": 3
      }
  }

This will require up to three structs, and is a bit tedious to type.
To speed up the process, simply paste the JSON into your IDE using the
rust-analyzer extension. A lightbulb icon should pop up that offers to
turn the JSON into matching structs. If you click on the icon, the JSON
will turn into the following code:

.. code-block:: rust

  #[derive(Serialize, Deserialize)]
  struct Struct2 {
      interval: i64,
      is_day: i64,
      temperature: f64,
      time: String,
      weathercode: i64,
      winddirection: i64,
      windspeed: f64,
  }
  #[derive(Serialize, Deserialize)]
  struct Struct3 {
      interval: String,
      is_day: String,
      temperature: String,
      time: String,
      weathercode: String,
      winddirection: String,
      windspeed: String,
  }
  #[derive(Serialize, Deserialize)]
  struct Struct1 {
      current_weather: Struct2,
      current_weather_units: Struct3,
      elevation: i64,
      generationtime_ms: f64,
      latitude: f64,
      longitude: i64,
      timezone: String,
      timezone_abbreviation: String,
      utc_offset_seconds: i64,
  }

With this, the only remaining work is to name the structs and made some
decisions on where to choose a different type from the automatically
generated parameters. The ``time`` parameter for example can be turned
into a ``LocalDatetime`` instead of a ``String``.

.. lint-off

Conversely, the unofficial
`edgedb-query-derive <https://docs.rs/edgedb-query-derive/latest/edgedb_query_derive/attr.select_query.html>`
crate provides a way to turn Rust types into EdgeQL queries using its
``.to_edge_query()`` method.

.. lint-on