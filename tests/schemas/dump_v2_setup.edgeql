INSERT Test1;  # https://github.com/edgedb/edgedb/issues/2606

INSERT Test2 {
    range_of_int := range(-1, 10),
    range_of_date := range(
        <datetime>'2010-12-27T23:59:59-07:00',
        <datetime>'2012-12-27T23:59:59-07:00',
    ),
    date_duration := <cal::date_duration>'1month 3days',
};

INSERT TargetA {name := 't0'};
INSERT TargetA {name := 't1'};
INSERT TargetA {name := 't2'};

INSERT SourceA {name := 's0', link1 := (SELECT TargetA FILTER .name = 't0')};
INSERT SourceA {name := 's1', link1 := (SELECT TargetA FILTER .name = 't1')};
INSERT SourceA {name := 's2', link1 := (SELECT TargetA FILTER .name = 't1')};
INSERT SourceA {name := 's3', link2 := (SELECT TargetA FILTER .name = 't2')};
INSERT SourceA {name := 's4', link2 := (SELECT TargetA FILTER .name = 't2')};
