{
  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs/nixos-unstable-small";

    flake-parts.url = "github:hercules-ci/flake-parts";

    fenix.url = "github:nix-community/fenix";
    fenix.inputs.nixpkgs.follows = "nixpkgs";
    fenix.inputs.rust-analyzer-src.follows = "";

    crane.url = "github:ipetkov/crane";
    crane.inputs.nixpkgs.follows = "nixpkgs";
  };
  outputs = {...} @ inputs:
    with builtins;
      inputs.flake-parts.lib.mkFlake {
        inherit inputs;
      } {
        systems = ["x86_64-linux"];
        perSystem = {
          config,
          system,
          self',
          inputs',
          ...
        }: let
          pkgs = inputs.nixpkgs.legacyPackages.${system};
          inherit (pkgs) lib stdenvNoCC;

          edgedbServerPackages = let
            # TODO: Wrap edgedb in derivation and add current version
            platforms = {
              x86_64-linux = {
                "4.2" = {
                  url = "https://packages.edgedb.com/archive/x86_64-unknown-linux-gnu/edgedb-server-4.2%2B8778fc1.tar.zst";
                  hash = "sha256-65AP0jNJ/DV6yqtSU8XM1nkZgZoKKedf0yjF0Ef3qOU=";
                };
                "4.6" = {
                  url = "https://packages.edgedb.com/archive/x86_64-unknown-linux-gnu/edgedb-server-4.6%2B0632dff.tar.zst";
                  hash = "sha256-ia4w2kZKO0PQOJCz6M8IL24L9GUNoKeeH+2j8ceeBmg=";
                };
                "5" = {
                  url = "https://packages.edgedb.com/archive/x86_64-unknown-linux-gnu.testing/edgedb-server-5.0-beta.1%2B15775e6.tar.zst";
                  hash = "sha256-Rtcu2+/04x7Dk6zwHr0VFB07yfc06vxrCjMkpCEXFcY=";
                };
              };
            };

            mkEdgedbServer = ver: content:
              stdenvNoCC.mkDerivation {
                name = "edgedb-server";
                buildInputs = [pkgs.python3];
                nativeBuildInputs = [pkgs.zstd] ++ lib.optionals (!stdenvNoCC.isDarwin) [pkgs.autoPatchelfHook];
                dontPatchELF = stdenvNoCC.isDarwin;
                dontFixup = stdenvNoCC.isDarwin;
                src = pkgs.fetchurl content;
                installPhase = "mkdir $out && cp -r ./* $out";
              };
          in
            mapAttrs mkEdgedbServer platforms.${system};

          edgedbCli = let
            platforms = {
              x86_64-linux = {
                url = "https://packages.edgedb.com/archive/x86_64-unknown-linux-musl/edgedb-cli-4.1.0%2B03ae624.zst";
                hash = "sha256-sAXa9YMuUVFBU/J6Ik1DjiKBLUmKGIhP9e3HEvBIuBw=";
              };
            };
          in
            stdenvNoCC.mkDerivation {
              name = "edgedb";
              nativeBuildInputs = [pkgs.zstd] ++ lib.optionals (!stdenvNoCC.isDarwin) [pkgs.autoPatchelfHook];

              dontPatchELF = stdenvNoCC.isDarwin;
              dontFixup = stdenvNoCC.isDarwin;
              src = pkgs.fetchurl platforms.${system};
              unpackPhase = ''
                runHook preUnpack
                zstd --decompress $src -o edgedb
                runHook postUnpack
              '';
              installPhase = ''
                mkdir -p $out/bin
                mv edgedb $out/bin/
                chmod +x $out/bin/edgedb
              '';
            };

          rustToolchain = with inputs.fenix.packages.${system};
            combine [
              latest.rustc
              latest.rust-src
              latest.cargo
            ];
        in {
          devShells.default = pkgs.mkShell {
            name = "edgedb dev shell";
            venvDir = "./venv";

            buildInputs = let
              edb = rec {
                port = toString 10703;
                dir = "./.edgedb";
                socket = "${dir}/.s.EDGEDB.admin.${port}";
                dsn = "edgedb://edgedb:edgedb@127.0.0.1:${port}/benchmarks";
                args = "--no-cli-update-check --tls-security=insecure --dsn=${dsn}";
              };

              scripts = let
                mkEdgedbScripts = version: let
                  pkg = edgedbServerPackages.${version};
                in {
                  "edgedb:${version}:start" = ''
                    ${lib.concatStringsSep " \\\n" [
                      "${pkg}/bin/edgedb-server"
                      "--data-dir=${edb.dir}"
                      "--pidfile-dir=${edb.dir}"
                      "--bind-address=127.0.0.1"
                      "--port=${edb.port}"
                      "--security=insecure_dev_mode"
                      "--admin-ui=disabled"
                      "--instance-name=${version}_benchmarks"
                      "--log-level=warn"
                    ]}
                  '';

                  "edgedb:${version}:prepare" = ''
                    edgedb:query 'CREATE DATABASE benchmarks' ||:
                    edgedb ${edb.args} restore ./dumps/mrfoxpro.dump
                  '';

                  "edgedb:${version}:bench" = ''
                    kill -s SIGTERM $(pgrep -f ${pkg}) > /dev/null 2>&1

                    edgedb:${version}:start &

                    trap 'kill -s SIGTERM $(pgrep -f ${pkg})' INT TERM

                    edgedb:${version}:prepare
                    EDGEDB_DSN=${edb.dsn} cargo run -p edgedb_benchmarks --release

                    wait
                  '';
                };
              in
                {
                  "commands" = with lib; let
                    commands = pipe scripts [
                      attrNames
                      (groupBy (cmd: elemAt (splitString ":" cmd) 0))
                      (mapAttrsToList (group: commands: let
                        splitted = pipe commands [
                          (sortOn stringLength)
                          (map (removePrefix group))
                          (concatStringsSep "|")
                        ];
                      in "$(tput setaf 50)${group}$(tput sgr0)|${splitted}"))
                      (intersperse "\n")
                      concatStrings
                    ];
                  in ''
                    echo "${commands}" | ${pkgs.unixtools.column}/bin/column --table -W 1 -T 1 -t -s "|"
                  '';
                  "edgedb:query" = ''${edgedbCli}/bin/edgedb query --unix-path=${edb.socket} -u edgedb --connect-timeout=5s "$@"'';
                }
                // (foldl' (acc: version: acc // (mkEdgedbScripts version)) {} (attrNames edgedbServerPackages));
            in
              with pkgs;
                []
                ++ [rustToolchain]
                ++ [edgedbCli]
                ++ (attrValues edgedbServerPackages)
                ++ (lib.mapAttrsToList writeShellScriptBin scripts);

            shellHook = ''
              echo "$(tput setaf 20)🌳 Welcome in EdgeDB environment$(tput sgr0)"
              commands
            '';
          };
        };
      };
}
