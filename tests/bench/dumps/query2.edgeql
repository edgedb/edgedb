with
  q := <optional str>$q,
  nickname := <optional str>$nickname,
  me_num := <int64>$me_num,
  users := (
    select User
    filter (.can_be_visible or .num = me_num)
      and (.nickname = nickname) ?? true
      and ((q ilike "%" ++ .nickname ++ "%") ?? true or (q ilike "%" ++ .name ++ "%") ?? true)
    ) if (exists q or exists nickname) 
    else (select User filter .can_be_visible and .num != me_num),
  deals := (select DealBase filter .owner in users),
  projects := (select Project filter exists (users intersect .participants)),
select {
    multi users := users { 
      num,
      name,
      nickname,
      picture_id,
      online,
      last_seen,
      can_be_visible,
    },
    multi deals := deals { 
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
    multi tags := distinct (deals.tags union projects.tags union users.interests) { 
      num,
      name,
      sphere,
      score,
      is_base,
    },
    multi projects := projects { 
      num,
      name,
      description,
      picture_url,
      website,
      score,

      owner_num := .owner.num,
      participant_nums := .participants.num,
      tag_nums := .tags.num,

      created_at,
    },
}