with
  q := str_trim(<optional str>$q),
  filtered := (
    with q_range := range(<optional int64>$lower, <optional int64>$upper),
    select DealBase 
    filter
      (<optional str>$sphere in .tags.sphere) ?? true
      and ([is VacancyDeal].area.full ilike "%" ++ <optional str>$geoarea ++ "%") ?? true 
      # and overlaps(q_range, range(.lower, .upper)) ?? true
  ),
      
  # TODO https://github.com/edgedb/edgedb/issues/6530
  fts := (
    select distinct (
      select fts::search(filtered, q, language := "rus")
      order by .score desc
    ).object
  ) if exists q else filtered,

  ws := (
    with
      tokens := array_unpack(str_split(q, " ")),
      similar_tags := (
        select (
          for token in (select tokens filter len(tokens) > 3) union (
            select Tag
            filter ext::pg_trgm::word_similar(token, .name)
          )
        )
      ),
    select filtered { matched_tags_count := count(similar_tags intersect .tags) } 
    filter .matched_tags_count > 0 
    order by .matched_tags_count desc
  ) if exists q else filtered,

  ml := (
    with 
      e := <SearchEmbeddings><optional array<float32>>$e,
    select  (
      select filtered { dist := ext::pgvector::cosine_distance(e, .embeddings) }
      filter .dist < 0.8
      order by .dist asc
    ) if exists e else {}
  ) if exists q else filtered,

  un := (ws union fts union ml),
select {
  fts := fts { 
    _kind := pure_typename(.__type__.name),
    num,
    owner_num := .owner.num,
    sphere,
    tag_nums := .tags.num,
    description,

    [is VacancyDeal].task,
    [is VacancyDeal].employee,
    [is VacancyDeal].offer_type,
    [is VacancyDeal].currency,
    [is VacancyDeal].lower,
    [is VacancyDeal].upper,
    area_full := [is VacancyDeal].area.full,
  
    [is VacancyDeal].is_remote,

    [is BeggingDeal].business 
  },
  ws := ws { 
    _kind := pure_typename(.__type__.name),
    num,
    owner_num := .owner.num,
    sphere,
    tag_nums := .tags.num,
    description,

    [is VacancyDeal].task,
    [is VacancyDeal].employee,
    [is VacancyDeal].offer_type,
    [is VacancyDeal].currency,
    [is VacancyDeal].lower,
    [is VacancyDeal].upper,
    area_full := [is VacancyDeal].area.full,
  
    [is VacancyDeal].is_remote,

    [is BeggingDeal].business
  },
  ml := ml { 
    _kind := pure_typename(.__type__.name),
    num,
    owner_num := .owner.num,
    sphere,
    tag_nums := .tags.num,
    description,

    [is VacancyDeal].task,
    [is VacancyDeal].employee,
    [is VacancyDeal].offer_type,
    [is VacancyDeal].currency,
    [is VacancyDeal].lower,
    [is VacancyDeal].upper,
    area_full := [is VacancyDeal].area.full,
  
    [is VacancyDeal].is_remote,

    [is BeggingDeal].business 
  },
  un_count := count(un),
  watched_deals := (select distinct un filter <int64>$me in .watched_by.num).num,
  users := un.owner { 
    num,
    name,
    nickname,
    picture_id,
    online,
    last_seen,
    can_be_visible  
  },
  tags := un.tags { 
    num,
    name,
    sphere,
    score,
    is_base
  },
}