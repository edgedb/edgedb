:orphan:

.. _ref_ai_vectorstore_python:

======================
Vectorstore Python API
======================


Core Classes
============

.. py:class:: GelVectorstore

    A framework-agnostic interface for interacting with |Gel's| ext::vectorstore.

    This class provides methods for storing, retrieving, and searching
    vector embeddings. It follows vector database conventions and supports
    different embedding models.

    Args:

    * ``embedding_model`` (:py:class:`BaseEmbeddingModel`): The embedding model used to generate vectors.
    * ``collection_name`` (str): The name of the collection.
    * ``record_type`` (str): The schema type (table name) for storing records.
    * ``client_config`` (dict | None): The config for the |Gel| client.


.. py:method:: add_items(self, items: list[InsertItem])

    Add multiple items to the vector store in a single transaction. Embeddings
    will be generated and stored for all items.

    Args:

    * ``items`` (list[:py:class:`InsertItem`]): List of items to add. Each contains:

      * ``text`` (str): The text content to be embedded
      * ``metadata`` (dict[str, Any]): Additional data to store

    Returns:

    * List of database record IDs for the inserted items.


.. py:method:: add_vectors(self, records: list[InsertRecord])

    Add pre-computed vector embeddings to the store. Use this method when you have
    already generated embeddings and want to store them directly without re-computing them.

    Args:

    * ``records`` (list[:py:class:`InsertRecord`]): List of records. Each contains:

      * ``embedding`` (list[float]): Pre-computed embeddings
      * ``text`` (Optional[str]): Original text content
      * ``metadata`` (dict[str, Any]): Additional data to store

    Returns:

    * List of database record IDs for the inserted items.

.. py:method:: delete(self, ids: list[uuid.UUID])

    Delete records from the vector store by their IDs.

    Args:

    * ``ids`` (list[uuid.UUID]): List of record IDs to delete.

    Returns:

    * List of deleted record IDs.

.. py:method:: get_by_ids(self, ids: list[uuid.UUID]) -> list[Record]

    Retrieve specific records by their IDs.

    Args:

    * ``ids`` (list[uuid.UUID]): List of record IDs to retrieve.

    Returns:

    * List of retrieved records. Each result contains:

      * ``id`` (uuid.UUID): The record's unique identifier
      * ``text`` (Optional[str]): The original text content
      * ``embedding`` (Optional[list[float]]): The stored vector embedding
      * ``metadata`` (Optional[dict[str, Any]]): Any associated metadata

.. py:method:: search_by_item(self, item: Any, filters: Optional[CompositeFilter] = None, limit: Optional[int] = 4) -> list[SearchResult]

    Search for similar items in the vector store.

    This method:

    1. Generates an embedding for the input item
    2. Finds records with similar embeddings
    3. Optionally filters results based on metadata
    4. Returns the most similar items up to the specified limit

    Args:

    * ``item`` (Any): The query item to find similar matches for. Must be compatible with the embedding model's target_type.
    * ``filters`` (Optional[:py:class:`CompositeFilter`]): Metadata-based filters to use.
    * ``limit`` (Optional[int]): Max number of results to return. Defaults to 4.

    Returns:

    * List of similar items, ordered by similarity. Each result contains:

      * ``id`` (uuid.UUID): The record's unique identifier
      * ``text`` (Optional[str]): The original text content
      * ``embedding`` (list[float]): The stored vector embedding
      * ``metadata`` (Optional[dict[str, Any]]): Any associated metadata
      * ``cosine_similarity`` (float): Similarity score (higher is more similar)

.. py:method:: search_by_vector(self, vector: list[float], filter_expression: str = "", limit: Optional[int] = 4) -> list[SearchResult]

    Search using a pre-computed vector embedding. Useful when you have already computed
    the embedding or want to search with a modified/combined embedding vector.

    Args:

    * ``vector`` (list[float]): The query embedding to search with. Must match the dimensionality of stored embeddings.
    * ``filter_expression`` (str): Filter expression for metadata filtering.
    * ``limit`` (Optional[int]): Max number of results to return. Defaults to 4.

    Returns:

    * List of similar items, ordered by similarity. Each result contains:

      * ``id`` (uuid.UUID): The record's unique identifier
      * ``text`` (Optional[str]): The original text content
      * ``embedding`` (list[float]): The stored vector embedding
      * ``metadata`` (Optional[dict[str, Any]]): Any associated metadata
      * ``cosine_similarity`` (float): Similarity score (higher is more similar)

.. py:method:: update_record(self, record: Record) -> Optional[uuid.UUID]

    Update an existing record in the vector store. Only specified fields will be updated.
    If text is provided but not embedding, a new embedding will be automatically generated.

    Args:

    * ``record`` (:py:class:`Record`):

      * ``id`` (uuid.UUID): The ID of the record to update
      * ``text`` (Optional[str]): New text content. If provided without embedding, a new embedding will be generated.
      * ``embedding`` (Optional[list[float]]): New vector embedding.
      * ``metadata`` (Optional[dict[str, Any]]): New metadata to store with the record. Completely replaces existing metadata.

    Returns:

    * The updated record's ID if found and updated, None if no record was found with the given ID.

    Raises:

    * ValueError: If no fields are specified for update.


.. py:class:: BaseEmbeddingModel

    Abstract base class for embedding models.
    Any embedding model used with :py:class:`GelVectorstore` must implement this
    interface. The model is expected to convert input data (text, images, etc.)
    into a numerical vector representation.

    .. py:method:: __call__(self, item) -> list[float]

        Convert an input item into a list of floating-point values (vector
        embedding). Must be implemented in subclasses.

        Args:

        * ``item``: Input item to be converted to an embedding

        Returns:

        * list[float]: Vector embedding of the input item

    .. py:method:: dimensions(self) -> int

        Return the number of dimensions in the embedding vector.
        Must be implemented in subclasses.

        Returns:

        * int: Number of dimensions in the embedding vector

    .. py:method:: target_type(self) -> TypeVar

        Return the expected data type of the input (e.g., str for text, image
        for vision models). Must be implemented in subclasses.

        Returns:

        * TypeVar: Expected input data type


Data Classes
============

.. py:class:: InsertItem

    An item whose embedding will be created and stored alongside the item in the vector store.

    Args:

    * ``text`` (str): The text content to be embedded
    * ``metadata`` (dict[str, Any]): Additional data to store. Defaults to empty dict.

.. py:class:: InsertRecord

    A record to be added to the vector store with embedding pre-computed.

    Args:

    * ``embedding`` (list[float]): Pre-computed embeddings
    * ``text`` (str | None): Original text content. Defaults to None.
    * ``metadata`` (dict[str, Any]): Additional data to store. Defaults to empty dict.

.. py:class:: Record

    A record retrieved from the vector store, or an update record.
    Custom ``__init__`` so we can detect which fields the user passed
    (even if they pass None or {}).

    Args:

    * ``id`` (uuid.UUID): The record's unique identifier
    * ``text`` (str | None): The text content. Defaults to None.
    * ``embedding`` (list[float] | None): The vector embedding. Defaults to None.
    * ``metadata`` (dict[str, Any]): Additional data stored with the record. Defaults to empty dict.

.. py:class:: SearchResult

    A search result from the vector store.

    Inherits from :py:class:`Record`

    Args:

    * ``cosine_similarity`` (float): Similarity score for the search result. Defaults to 0.0.


Metadata Filtering
==================

.. py:class:: FilterOperator

    Enumeration of supported filter operators for metadata filtering.

    Values:

    * ``EQ``: Equal to (=)
    * ``NE``: Not equal to (!=)
    * ``GT``: Greater than (>)
    * ``LT``: Less than (<)
    * ``GTE``: Greater than or equal to (>=)
    * ``LTE``: Less than or equal to (<=)
    * ``IN``: Value in array
    * ``NOT_IN``: Value not in array
    * ``LIKE``: Pattern matching
    * ``ILIKE``: Case-insensitive pattern matching
    * ``ANY``: Any array element matches
    * ``ALL``: All array elements match
    * ``CONTAINS``: String contains value
    * ``EXISTS``: Field exists

.. py:class:: FilterCondition

    Enumeration of conditions for combining multiple filters.

    Values:

    * ``AND``: All conditions must be true
    * ``OR``: Any condition must be true

.. py:class:: MetadataFilter

    Represents a single metadata filter condition.

    Args:

    * ``key`` (str): The metadata field key to filter on
    * ``value`` (int | float | str): The value to compare against
    * ``operator`` (:py:class:`FilterOperator`): The comparison operator. Defaults to FilterOperator.EQ.

.. py:class:: CompositeFilter

    Allows grouping multiple MetadataFilter instances using AND/OR conditions.

    Args:

    * ``filters`` (list[:py:class:`CompositeFilter` | :py:class:`MetadataFilter`]): List of filters to combine
    * ``condition`` (:py:class:`FilterCondition`): How to combine the filters. Defaults to FilterCondition.AND.

.. py:function:: get_filter_clause(filters: CompositeFilter) -> str

    Get the filter clause for a given CompositeFilter.

    Args:

    * ``filters`` (:py:class:`CompositeFilter`): The composite filter to convert to a clause

    Returns:

    * str: The filter clause string for use in queries

    Raises:

    * ValueError: If an unknown operator or condition is encountered

