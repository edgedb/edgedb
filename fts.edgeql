configure current database set allow_bare_ddl := 'AlwaysAllow';

drop type BlogPost;
drop type Post;

create type Post {
    create property title -> str;

    create index fts::textsearch on (
        fts::with_language(.title, fts::Language.English)
    );
};

create type BlogPost extending Post {
    create property body -> str;

    create index fts::textsearch on ((
        fts::with_language(.title, fts::Language.English),
        fts::with_language(.body, fts::Language.English)
    ));
}

# insert Post {
#     title := "when things don’t satisfies requirements",
#     body := "Being part of the software industry for over fifteen years now, I understand that expecting your favourite software/hardware"
# };
# insert Post {
#     title := "a conference about technological apples",
#     body := "Watching WWDC with a few friends is a yearly tradition. From watching it in person in Mumbai to now having conversations on iMessage about the latest updates to Apple’s ecosystem. I look forward to this day."
# };
# insert Post {
#     title := "thank you internet, for every little thing",
#     body := "I should make this a regular post on my blog where I thank the internet and everyone who contributes to keeping things runnings."
# };
# 
# select Post { title } filter fts::test("satisfy", Post);
# select Post { title } filter fts::test("thing", Post);

# alter type Post {
#     drop index fts::textsearch;
# }

