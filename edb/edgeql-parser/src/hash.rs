use sha2::digest::Digest;

use crate::position::Pos;
use crate::tokenizer::Tokenizer;

#[derive(Debug, Clone)]
pub struct Hasher {
    hasher: sha2::Sha256,
}

#[derive(Debug)]
pub enum Error {
    // TODO: use [crate::Error] instead
    Tokenizer(String, Pos),
}

impl Hasher {
    pub fn start_migration(parent_id: &str) -> Hasher {
        let mut me = Hasher {
            hasher: sha2::Sha256::new(),
        };
        me.hasher.update(b"CREATE\0MIGRATION\0ONTO\0");
        me.hasher.update(parent_id.as_bytes());
        me.hasher.update(b"\0{\0");
        me
    }
    pub fn add_source(&mut self, data: &str) -> Result<&mut Self, Error> {
        let mut parser = &mut Tokenizer::new(data);
        for token in &mut parser {
            let token = match token {
                Ok(t) => t,
                Err(crate::tokenizer::Error { message, .. }) => {
                    return Err(Error::Tokenizer(message, parser.current_pos()));
                }
            };
            self.hasher.update(token.text.as_bytes());
            self.hasher.update(b"\0");
        }
        Ok(self)
    }
    pub fn make_migration_id(mut self) -> String {
        self.hasher.update(b"}\0");
        let hash = base32::encode(
            base32::Alphabet::Rfc4648 { padding: false },
            &self.hasher.finalize(),
        );
        format!("m1{}", hash.to_ascii_lowercase())
    }
}

#[cfg(test)]
mod test {
    use super::Hasher;

    fn hash(initial: &str, text: &str) -> String {
        let mut hasher = Hasher::start_migration(initial);
        hasher.add_source(text).unwrap();
        hasher.make_migration_id()
    }

    #[test]
    fn empty() {
        assert_eq!(
            hash("initial", "    \n   "),
            "m1tjyzfl33vvzwjd5izo5nyp4zdsekyvxpdm7zhtt5ufmqjzczopdq"
        );
    }

    #[test]
    fn hash_1() {
        assert_eq!(
            hash(
                "m1g3qzqdr57pp3w2mdwdkq4g7dq4oefawqdavzgeiov7fiwntpb3lq",
                r###"
                CREATE TYPE Type1;
            "###
            ),
            "m1fvpcra5cxntkss3k2to2yfu7pit3t3owesvdw2nysqvvpihdiszq"
        );
    }

    #[test]
    fn tokens_arent_normalized() {
        assert_eq!(
            hash(
                "m1g3qzqdr57pp3w2mdwdkq4g7dq4oefawqdavzgeiov7fiwntpb3lq",
                r###"
                CREATE type Type1;
            "###
            ),
            "m1ddghtidugdk3mazwfzpfblqzuoqvsxpivgy2fbq4vywykab7z5rq"
        );

        assert_eq!(
            hash(
                "m1g3qzqdr57pp3w2mdwdkq4g7dq4oefawqdavzgeiov7fiwntpb3lq",
                r###"
                creATE TyPe Type1;
            "###
            ),
            "m1oc32ytxeqlvxeyps3ozqiqazy2duuz5bcqog7nkhubmkbsjgf4vq"
        );
    }

    #[test]
    fn hash_parent() {
        assert_eq!(
            hash(
                "initial",
                r###"
                CREATE TYPE Type1;
            "###
            ),
            "m1q3jjfe7zjl74v3n2vxjwzneousdas6vvd4qwrfd6j6xmhmktyada"
        );
    }
}
