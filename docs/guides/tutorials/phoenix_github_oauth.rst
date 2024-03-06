.. _ref_guide_phoenix_github_oauth:

=======
Phoenix
=======

:edb-alt-title: Building a GitHub OAuth application


In this tutorial, we'll look at
how you can create an application with authorization through GitHub using
`Phoenix <https://phoenixframework.org/>`_ and :ref:`the official EdgeDB Elixir
driver <edgedb-elixir-intro>`.

This tutorial is a simplified version of the `LiveBeats
<https://github.com/fly-apps/live_beats>`_ application from
`fly.io <https://fly.io>`_ with EdgeDB instead of PostgreSQL, which focuses on
implementing authorization via GitHub. The completed implementation of this
example can be found `on GitHub <repository_>`_. The full version
of LiveBeats version on EdgeDB can also be found `on GitHub
<https://github.com/nsidnev/edgedb-phoenix-example>`_

.. _repository:
    https://github.com/edgedb/edgedb-examples/tree/main/phoenix-github-oauth

.. _prerequisites:

Prerequisites
=============

For this tutorial we will need:

* EdgeDB CLI.
* Elixir version 1.13 or higher.
* Phoenix framework version 1.6 or higher.
* `GitHub OAuth application <gh-oauth-guide_>`_.

.. _gh-oauth-guide:
    https://docs.github.com/
    en/developers/apps/building-oauth-apps/creating-an-oauth-app

Before discussing the project database schema, let's generate a skeleton for
our application. We will make sure that it will use binary IDs for the Ecto
schemas because EdgeDB uses UUIDs as primary IDs, which in Elixir are
represented as strings, and since it is basically a plain JSON API application,
we will disable all the built-in Phoenix integrations.

.. code-block:: bash

  $ mix phx.new phoenix-github_oauth --app github_oauth --module GitHubOAuth \
  >   --no-html --no-gettext --no-dashboard --no-live --no-mailer --binary-id
  $ cd phoenix-github_oauth/

Let's also get rid of some default things that were created by Phoenix and
won't be used by us.

.. code-block:: bash

  $ # remove the module Ecto.Repo and the directory for Ecto migrations,
  $ # because they will not be used
  $ rm -r lib/github_oauth/repo.ex priv/repo/

And then add the EdgeDB driver, the ``Ecto`` helper for it and the ``Mint``
HTTP client for GitHub OAuth client as project dependencies to ``mix.exs``.

.. code-block:: elixir

  defmodule GitHubOAuth.MixProject do
    # ...

    defp deps do
      [
        {:phoenix, "~> 1.6.9"},
        {:phoenix_ecto, "~> 4.4"},
        {:esbuild, "~> 0.4", runtime: Mix.env() == :dev},
        {:telemetry_metrics, "~> 0.6"},
        {:telemetry_poller, "~> 1.0"},
        {:jason, "~> 1.2"},
        {:plug_cowboy, "~> 2.5"},
        {:edgedb, "~> 0.3.0"},
        {:edgedb_ecto, git: "https://github.com/nsidnev/edgedb_ecto"},
        {:mint, "~> 1.0"}  # we need mint to write the GitHub client
      ]
    end

    # ...
  end

Now we need to download new dependencies.

.. code-block:: bash

  $ mix deps.get

Next, we will create a module in ``lib/github_oauth/edgedb.ex`` which will
define a child specification for the EdgeDB driver and use the ``EdgeDBEcto``
helper, which will inspect the queries that will be stored in the
``priv/edgeql/`` directory and generate Elixir code for them.

.. code-block:: elixir

  defmodule GitHubOAuth.EdgeDB do
    use EdgeDBEcto,
      name: __MODULE__,
      queries: true,
      otp_app: :github_oauth

    def child_spec(_opts \\ []) do
      %{
        id: __MODULE__,
        start: {EdgeDB, :start_link, [[name: __MODULE__]]}
      }
    end
  end

Now we need to add ``GitHubOAuth.EdgeDB`` as a child for our application in
``lib/github_oauth/application.ex`` (at the same time removing the child
definition for ``Ecto.Repo`` from there).

.. code-block:: elixir

  defmodule GitHubOAuth.Application do
    # ...

    @impl true
    def start(_type, _args) do
      children = [
        # Start the EdgeDB driver
        GitHubOAuth.EdgeDB,
        # Start the Telemetry supervisor
        GitHubOAuthWeb.Telemetry,
        # Start the PubSub system
        {Phoenix.PubSub, name: GitHubOAuth.PubSub},
        # Start the Endpoint (http/https)
        GitHubOAuthWeb.Endpoint
        # Start a worker by calling: GitHubOAuth.Worker.start_link(arg)
        # {GitHubOAuth.Worker, arg}
      ]

      # ...
    end

    # ...
  end


Now we are ready to start working with EdgeDB! First, let's initialize a new
project for this application.

.. code-block:: bash

  $ edgedb project init
  No `edgedb.toml` found in `/home/<user>/phoenix-github_oauth` or above

  Do you want to initialize a new project? [Y/n]
  > Y

  Specify the name of EdgeDB instance to use with this project
  [default: phoenix_github_oauth]:
  > github_oauth

  Checking EdgeDB versions...
  Specify the version of EdgeDB to use with this project [default: 2.x]:
  > 2.x

  Do you want to start instance automatically on login? [y/n]
  > y

Great! Now we are ready to develop the database schema for the application.

Schema design
=============

This application will have 2 types: ``User`` and ``Identity``. The
``default::User`` represents the system user and the ``default::Identity``
represents the way the user logs in to the application (in this example via
GitHub OAuth).

This schema will be stored in a single EdgeDB module inside the
``dbschema/default.esdl`` file.

.. code-block:: sdl

  module default {
    type User {
      property name -> str;
      required property username -> str;
      required property email -> cistr;

      property profile_tagline -> str;

      property avatar_url -> str;
      property external_homepage_url -> str;

      required property inserted_at -> cal::local_datetime {
        default := cal::to_local_datetime(datetime_current(), 'UTC');
      }

      required property updated_at -> cal::local_datetime {
        default := cal::to_local_datetime(datetime_current(), 'UTC');
      }

      index on (.email);
      index on (.username);
    }

    type Identity {
      required property provider -> str;
      required property provider_token -> str;
      required property provider_login -> str;
      required property provider_email -> str;
      required property provider_id -> str;

      required property provider_meta -> json {
        default := <json>"{}";
      }

      required property inserted_at -> cal::local_datetime {
        default := cal::to_local_datetime(datetime_current(), 'UTC');
      }

      required property updated_at -> cal::local_datetime {
        default := cal::to_local_datetime(datetime_current(), 'UTC');
      }

      required link user -> User {
        on target delete delete source;
      }

      index on (.provider);
      constraint exclusive on ((.user, .provider));
    }
  }

After saving the file, we can create a migration for the schema and apply the
generated migration.

.. code-block:: bash

  $ edgedb migration create
  did you create object type 'default::User'? [y,n,l,c,b,s,q,?]
  > y

  did you create object type 'default::Identity'? [y,n,l,c,b,s,q,?]
  > y

  Created ./dbschema/migrations/00001.edgeql, id:
  m1yehm3jhj6jqwguelek54jzp4wqvvqgrcnvncxwb7676ult7nmcta

  $ edgedb migrate

Ecto schemas
============

In this tutorial we will define 2 ``Ecto.Schema`` modules, for
``default::User`` and ``default::Identity`` types, so that we can work with
EdgeDB in a more convenient way that is familiar to the world of Elixir.

Here is the definition for the user in the ``lib/accounts/user.ex`` file.

.. code-block:: elixir

  defmodule GitHubOAuth.Accounts.User do
    use Ecto.Schema
    use EdgeDBEcto.Mapper

    alias GitHubOAuth.Accounts.Identity

    @primary_key {:id, :binary_id, autogenerate: false}

    schema "default::User" do
      field :email, :string
      field :name, :string
      field :username, :string
      field :avatar_url, :string
      field :external_homepage_url, :string

      has_many :identities, Identity

      timestamps()
    end
  end

And here for identity in ``lib/accounts/identity.ex``.

.. code-block:: elixir

  defmodule GitHubOAuth.Accounts.Identity do
    use Ecto.Schema
    use EdgeDBEcto.Mapper

    alias GitHubOAuth.Accounts.User

    @primary_key {:id, :binary_id, autogenerate: false}

    schema "default::Identity" do
      field :provider, :string
      field :provider_token, :string
      field :provider_email, :string
      field :provider_login, :string
      field :provider_name, :string, virtual: true
      field :provider_id, :string
      field :provider_meta, :map

      belongs_to :user, User

      timestamps()
    end
  end

User authentication via GitHub
==================================

This part will be pretty big, as we'll talk about using ``Ecto.Changeset``
with the EdgeDB driver, as well as modules and queries related to user
registration via GitHub OAuth.

``Ecto`` provides "changesets" (via ``Ecto.Changeset``), which are convenient
to use when working with ``Ecto.Schema`` to validate external parameters. We
could use them via ``EdgeDBEcto`` instead, though not quite as fully as we can
with the full-featured adapters for ``Ecto``.

First, we will update the ``GitHubOAuth.Accounts.Identity`` module so that it
checks all the necessary parameters when we are creating a user via a GitHub
registration.

.. code-block:: elixir

  defmodule GitHubOAuth.Accounts.Identity do
    # ...
    import Ecto.Changeset

    alias GitHubOAuth.Accounts.{Identity, User}

    @github "github"

    # ...

    def github_registration_changeset(info, primary_email, emails, token) do
      params = %{
        "provider_token" => token,
        "provider_id" => to_string(info["id"]),
        "provider_login" => info["login"],
        "provider_name" => info["name"] || info["login"],
        "provider_email" => primary_email
      }

      %Identity{}
      |> cast(params, [
        :provider_token,
        :provider_email,
        :provider_login,
        :provider_name,
        :provider_id
      ])
      |> put_change(:provider, @github)
      |> put_change(:provider_meta, %{"user" => info, "emails" => emails})
      |> validate_required([
        :provider_token,
        :provider_email,
        :provider_name,
        :provider_id
      ])
    end
  end

And now let's define a changeset for user registration, which will use an
already defined changeset from ``GitHubOAuth.Accounts.Identity``.

.. code-block:: elixir

  defmodule GitHubOAuth.Accounts.User do
    # ...

    import Ecto.Changeset

    alias GitHubOAuth.Accounts.{User, Identity}

    # ...

    def github_registration_changeset(info, primary_email, emails, token) do
      %{
        "login" => username,
        "avatar_url" => avatar_url,
        "html_url" => external_homepage_url
      } = info

      identity_changeset =
        Identity.github_registration_changeset(
          info,
          primary_email,
          emails,
          token
        )

      if identity_changeset.valid? do
        params = %{
          "username" => username,
          "email" => primary_email,
          "name" => get_change(identity_changeset, :provider_name),
          "avatar_url" => avatar_url,
          "external_homepage_url" => external_homepage_url
        }

        %User{}
        |> cast(params, [
          :email,
          :name,
          :username,
          :avatar_url,
          :external_homepage_url
        ])
        |> validate_required([:email, :name, :username])
        |> validate_username()
        |> validate_email()
        |> put_assoc(:identities, [identity_changeset])
      else
        %User{}
        |> change()
        |> Map.put(:valid?, false)
        |> put_assoc(:identities, [identity_changeset])
      end
    end

    defp validate_email(changeset) do
      changeset
      |> validate_required([:email])
      |> validate_format(
        :email,
        ~r/^[^\s]+@[^\s]+$/,
        message: "must have the @ sign and no spaces"
      )
      |> validate_length(:email, max: 160)
    end

    defp validate_username(changeset) do
      validate_format(changeset, :username, ~r/^[a-zA-Z0-9_-]{2,32}$/)
    end
  end

Now that we have the schemas and changesets defined, let's define a set of the
EdgeQL queries we need for the login process.

There are 5 queries that we will need:

1. Search for a user by user ID.

2. Search for a user by email and by identity provider.

3. Update the identity token if the user from the 1st query exists.

4. Registering a user along with his identity data, if the 1st request did not
   return the user.

5. Querying a user identity before updating its token.

Before writing the queries themselves, let's create a context module
``lib/github_oauth/accounts.ex`` that will use these queries, and the module
itself will be used by Phoenix controllers.

.. code-block:: elixir

  defmodule GitHubOAuth.Accounts do
    import Ecto.Changeset

    alias GitHubOAuth.Accounts.{User, Identity}

    def get_user(id) do
      GitHubOAuth.EdgeDB.Accounts.get_user_by_id(id: id)
    end

    def register_github_user(primary_email, info, emails, token) do
      if user = get_user_by_provider(:github, primary_email) do
        update_github_token(user, token)
      else
        info
        |> User.github_registration_changeset(primary_email, emails, token)
        |> EdgeDBEcto.insert(
          &GitHubOAuth.EdgeDB.Accounts.register_github_user/1,
          nested: true
        )
      end
    end

    def get_user_by_provider(provider, email) when provider in [:github] do
      GitHubOAuth.EdgeDB.Accounts.get_user_by_provider(
        provider: to_string(provider),
        email: String.downcase(email)
      )
    end

    defp update_github_token(%User{} = user, new_token) do
      identity =
        GitHubOAuth.EdgeDB.Accounts.get_identity_for_user(
          user_id: user.id,
          provider: "github"
        )

      {:ok, _} =
        identity
        |> change()
        |> put_change(:provider_token, new_token)
        |> EdgeDBEcto.update(
          &GitHubOAuth.EdgeDB.Accounts.update_identity_token/1
        )

      identity = %Identity{identity | provider_token: new_token}
      {:ok, %User{user | identities: [identity]}}
    end
  end

Note that updating a token with a single query is quite easy, but we will use
two separate queries, to show how to work with ``Ecto.Changeset`` in different
ways.

Now that all the preparations are complete, we can start writing EdgeQL
queries.

We start with the ``priv/edgeql/accounts/get_user_by_provider.edgeql`` file,
which defines a query to find an user with a specified email provider.

.. code-block:: edgeql

  # edgedb = :query_single!
  # mapper = GitHubOAuth.Accounts.User

  select User {
    id,
    name,
    username,
    email,
    avatar_url,
    external_homepage_url,
    inserted_at,
    updated_at,
  }
  filter
    .<user[is Identity].provider = <str>$provider
      and
    str_lower(.email) = str_lower(<str>$email)
  limit 1

It is worth noting the ``# edgedb = :query_single!`` and
``# mapper = GitHubOAuth.Accounts.User`` comments. Both are special comments
that will be used by ``EdgeDBEcto`` when generating query functions. The
``edgedb`` comment defines the driver function for requesting data.
Information on all supported features can be found in the driver
`documentation <https://hexdocs.pm/edgedb/EdgeDB.html#functions>`_.
The ``mapper`` comment is used to define the module that will be used to map
the result from EdgeDB to some other form. Our ``Ecto.Schema`` schemas support
this with ``use EdgeDBEcto.Mapper`` expression at the top of the module
definition.

The queries for `getting the identity <get-identity-query_>`_ and
`getting the user by ID <get-user-by-id-query_>`_ are quite similar to the
above, so we will omit them here. You can find these queries in the
`example repository <repository_>`_.

.. _get-identity-query:
    https://github.com/edgedb/edgedb-examples/blob/main/
    phoenix-github-oauth/priv/edgeql/accounts/get_identity_for_user.edgeql

.. _get-user-by-id-query:
    https://github.com/edgedb/edgedb-examples/blob/main/
    phoenix-github-oauth/priv/edgeql/accounts/get_user_by_id.edgeql

Instead, let's look at how to update the user identity. This will be described
in the ``priv/edgeql/accounts/update_identity_token.edgeql`` file.

.. code-block:: edgeql

  # edgedb = :query_required_single

  with params := <json>$params
  update Identity
  filter .id = <uuid>params["id"]
  set {
    provider_token := (
      <str>json_get(params, "provider_token") ?? .provider_token
    ),
    updated_at := cal::to_local_datetime(datetime_current(), 'UTC'),
  }

As you can see, this query uses the named parameter ``$params`` instead of two
separate parameters such as ``$id`` and ``$provider_token``. This is because
to update our identity we use the changeset in the module
``GitHubOAuth.Accounts``, which automatically monitors changes to the schema
and will not give back the parameters, which will not affect the state of the
schema in update. So ``EdgeDBEcto`` automatically converts data from
changesets when it is an update or insert operation into a named ``$params``
parameter of type JSON. It also helps to work with nested changesets, as we
will see in the next query, which is defined in the
``priv/edgeql/accounts/register_github_user.edgeql`` file.

.. code-block:: edgeql

  # edgedb = :query_single!
  # mapper = GitHubOAuth.Accounts.User

  with
    params := <json>$params,
    identities_params := params["identities"],
    user := (
      insert User {
        email := <str>params["email"],
        name := <str>params["name"],
        username := <str>params["username"],
        avatar_url := <optional str>json_get(params, "avatar_url"),
        external_homepage_url := (
          <str>json_get(params, "external_homepage_url")
        ),
      }
    ),
    identites := (
      for identity_params in json_array_unpack(identities_params) union (
        insert Identity {
          provider := <str>identity_params["provider"],
          provider_token := <str>identity_params["provider_token"],
          provider_email := <str>identity_params["provider_email"],
          provider_login := <str>identity_params["provider_login"],
          provider_id := <str>identity_params["provider_id"],
          provider_meta := <json>identity_params["provider_meta"],
          user := user,
        }
      )
    )
  select user {
    id,
    name,
    username,
    email,
    avatar_url,
    external_homepage_url,
    inserted_at,
    updated_at,
    identities := identites,
  }

Awesome! We're almost done with our application!

As a final step in this tutorial, we will add 2 routes for the web
application. The first will redirect the user to the GitHub OAuth page if
they're not already logged in or will show their username otherwise. The second
is for logging into the application through GitHub.

Save the GitHub OAuth credentials from the `prerequisites <prerequisites_>`_
step as ``GITHUB_CLIENT_ID`` and ``GITHUB_CLIENT_SECRET`` environment
variables.

And then modify your ``config/dev.exs`` configuration file to use them.

.. code-block:: elixir

  # ...

  config :github_oauth, :github,
    client_id: System.fetch_env!("GITHUB_CLIENT_ID"),
    client_secret: System.fetch_env!("GITHUB_CLIENT_SECRET")

  # ...

First we create a file ``lib/github_oauth_web/controllers/user_controller.ex``
with a controller which will show the name of the logged in user or redirect
to the authentication page otherwise.

.. code-block:: elixir

  defmodule GitHubOAuthWeb.UserController do
    use GitHubOAuthWeb, :controller

    alias GitHubOAuth.Accounts

    plug :fetch_current_user

    def index(conn, _params) do
      if conn.assigns.current_user do
        json(conn, %{name: conn.assigns.current_user.name})
      else
        redirect(conn, external: GitHubOAuth.GitHub.authorize_url())
      end
    end

    defp fetch_current_user(conn, _opts) do
      user_id = get_session(conn, :user_id)
      user = user_id && Accounts.get_user(user_id)
      assign(conn, :current_user, user)
    end
  end

Note that the implementation of the ``GitHubOAuth.GitHub`` module is not given
here because it is relatively big and not a necessary part of this guide. If
you want to explore its internals, you can check out its implementation `on
GitHub <gh-client_>`_.

.. _gh-client:
    https://github.com/edgedb/edgedb-examples/blob/main/
    phoenix-github-oauth/lib/github_oauth/github.ex

Now add an authentication controller in
``lib/github_oauth_web/controllers/oauth_callback_controller.ex``.

.. code-block:: elixir

  defmodule GitHubOAuthWeb.OAuthCallbackController do
    use GitHubOAuthWeb, :controller

    alias GitHubOAuth.Accounts

    require Logger

    def new(
          conn,
          %{"provider" => "github", "code" => code, "state" => state}
        ) do
      client = github_client(conn)

      with {:ok, info} <-
            client.exchange_access_token(code: code, state: state),
          %{
            info: info,
            primary_email: primary,
            emails: emails,
            token: token
          } = info,
          {:ok, user} <-
            Accounts.register_github_user(primary, info, emails, token) do
        conn
        |> log_in_user(user)
        |> redirect(to: "/")
      else
        {:error, %Ecto.Changeset{} = changeset} ->
          Logger.debug("failed GitHub insert #{inspect(changeset.errors)}")

          error =
            "We were unable to fetch the necessary information from " <>
              "your GitHub account"

          json(conn, %{error: error})

        {:error, reason} ->
          Logger.debug("failed GitHub exchange #{inspect(reason)}")

          json(conn, %{
            error: "We were unable to contact GitHub. Please try again later"
          })
      end
    end

    def new(conn, %{"provider" => "github", "error" => "access_denied"}) do
      json(conn, %{error: "Access denied"})
    end

    defp github_client(conn) do
      conn.assigns[:github_client] || GitHubOAuth.GitHub
    end

    defp log_in_user(conn, user) do
      conn
      |> assign(:current_user, user)
      |> configure_session(renew: true)
      |> clear_session()
      |> put_session(:user_id, user.id)
    end
  end

Finally, we need to change ``lib/github_oauth_web/router.ex`` and add new
controllers there.

.. code-block:: elixir

  defmodule GitHubOAuthWeb.Router do
    # ...

    pipeline :api do
      # ...
      plug :fetch_session
    end

    scope "/", GitHubOAuthWeb do
      pipe_through :api

      get "/", UserController, :index
      get "/oauth/callbacks/:provider", OAuthCallbackController, :new
    end

    # ...
  end


Running web server
==================

That's it! Now we are ready to run our application and check if everything
works as expected.

.. code-block:: bash

  $ mix phx.server
  Generated github_oauth app
  [info] Running GitHubOAuthWeb.Endpoint with cowboy 2.9.0 at 127.0.0.1:4000
  (http)

  [info] Access GitHubOAuthWeb.Endpoint at http://localhost:4000

After going to http://localhost:4000, we will be greeted by the GitHub
authentication page. And after confirming the login we will be automatically
redirected back to our local server, which will save the received user in the
session and return the obtained user name in the JSON response.

We can also verify that everything is saved correctly by manually checking
the database data.

.. code-block:: edgeql-repl

  edgedb> select User {
  .......   name,
  .......   username,
  .......   avatar_url,
  .......   external_homepage_url,
  ....... };
  {
    default::User {
      name: 'Nik',
      username: 'nsidnev',
      avatar_url: 'https://avatars.githubusercontent.com/u/22559461?v=4',
      external_homepage_url: 'https://github.com/nsidnev'
    },
  }
  edgedb> select Identity {
  .......   provider,
  .......   provider_login
  ....... }
  ....... filter .user.username = 'nsidnev';
  {default::Identity {provider: 'github', provider_login: 'nsidnev'}}
