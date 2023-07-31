#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2023-present MagicStack Inc. and the EdgeDB authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


INSERT Status {
    name := 'Open'
};

INSERT Status {
    name := 'Closed'
};


INSERT User {
    name := 'Elvis'
};

INSERT User {
    name := 'Yury'
};

INSERT URL {
    name := 'edgedb.com',
    address := 'https://edgedb.com'
};

INSERT File {
    name := 'screenshot.png'
};

INSERT LogEntry {
    owner := (
        SELECT
            User {
                @note := 'reassigned'
            }
        FILTER
            User.name = 'Elvis'
    ),
    spent_time := 50000,
    body := 'Rewriting everything.'
};

INSERT Issue {
    number := '1',
    name := 'Release EdgeDB',
    body := 'Initial public release of EdgeDB.',
    owner := (SELECT User {
                @since := <datetime>'2018-01-01T00:00+00',
                @note := 'automatic assignment',
              }
              FILTER User.name = 'Elvis'),
    watchers := (SELECT User FILTER User.name = 'Yury'),
    status := (SELECT Status FILTER Status.name = 'Open'),
    time_spent_log := (SELECT LogEntry),
    time_estimate := 3000
};

INSERT Comment {
    body := 'EdgeDB needs to happen soon.',
    owner := (SELECT User FILTER User.name = 'Elvis'),
    issue := (SELECT Issue FILTER Issue.number = '1')
};


INSERT Issue {
    number := '2',
    name := 'Improve EdgeDB repl output rendering.',
    body := 'We need to be able to render data in tabular format.',
    owner := (SELECT User FILTER User.name = 'Yury'),
    watchers := (SELECT User FILTER User.name = 'Elvis'),
    status := (SELECT Status FILTER Status.name = 'Open'),
    references :=
        (SELECT URL FILTER URL.address = 'https://edgedb.com')
        UNION
        (SELECT File FILTER File.name = 'screenshot.png')
};

WITH
    I := DETACHED Issue
INSERT Issue {
    number := '3',
    name := 'Repl tweak.',
    body := 'Minor lexer tweaks.',
    owner := (SELECT User FILTER User.name = 'Yury'),
    watchers := (SELECT User FILTER User.name = 'Elvis'),
    status := (SELECT Status FILTER Status.name = 'Closed'),
    related_to := (
        SELECT I FILTER I.number = '2'
    ),
};

WITH
    I := DETACHED Issue
INSERT Issue {
    number := '4',
    name := 'Regression.',
    body := 'Fix regression introduced by lexer tweak.',
    owner := (SELECT User FILTER User.name = 'Elvis'),
    status := (SELECT Status FILTER Status.name = 'Closed'),
    related_to := (
        SELECT I FILTER I.number = '3'
    ),
};

# NOTE: UPDATE Users for testing the link properties
#
UPDATE User
FILTER User.name = 'Elvis'
SET {
    todo := (SELECT Issue FILTER Issue.number IN {'1', '2'})
};

UPDATE User
FILTER User.name = 'Yury'
SET {
    todo := (SELECT Issue FILTER Issue.number IN {'3', '4'})
};


# Make the database more populated so that it uses indexes...
for i in range_unpack(range(1, 1000)) union (
  with u := (insert User { name := <str>i }),
  for j in range_unpack(range(0, 5)) union (
    insert Issue {
      owner := u,
      number := <str>(i*100 + j),
      name := "issue " ++ <str>i ++ "/" ++ <str>j,
      status := (select Status filter .name = 'Open'),
      body := "BUG",
    }
));
update User set {
  todo += (select .owned_issues filter <int64>.number % 3 = 0)
};


for x in range_unpack(range(0, 500_000)) union (
  insert RangeTest {
    rval := range(-<int64>round(300*random()), <int64>round(300*random())),
    mval := multirange([
      range(-<int64>round(300*random()), <int64>round(300*random())),
      range(-<int64>round(300*random()), <int64>round(300*random())),
    ]),
    rdate := range(
      <cal::local_date>'2000-01-01' +
        cal::to_date_duration(days:=<int64>round(5000*random())),
      <cal::local_date>'2014-01-01' +
        cal::to_date_duration(days:=<int64>round(5000*random())),
    ),
    mdate := multirange([
      range(
        <cal::local_date>'2000-01-01' +
          cal::to_date_duration(days:=<int64>round(5000*random())),
        <cal::local_date>'2014-01-01' +
          cal::to_date_duration(days:=<int64>round(5000*random())),
      ),
      range(
        <cal::local_date>'2000-01-01' +
          cal::to_date_duration(days:=<int64>round(5000*random())),
        <cal::local_date>'2014-01-01' +
          cal::to_date_duration(days:=<int64>round(5000*random())),
      ),
    ]),
  }
);


for x in range_unpack(range(0, 100_000)) union (
    insert JSONTest{val := <json>(a:=x, b:=round(random()*1000))}
);


administer statistics_update();
