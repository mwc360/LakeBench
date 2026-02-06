{
  description = "LakeBench dev environment";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, flake-utils }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = import nixpkgs { inherit system; };
        python = pkgs.python313;
      in
      {
        devShells.default = pkgs.mkShell {
          packages = [
            python
            pkgs.uv
            pkgs.ruff
          ];

          shellHook = ''
            if [ ! -d .venv ]; then
              echo "Creating venv..."
              uv venv --python ${python}/bin/python .venv
              echo "Installing lakebench with all extras..."
              uv pip install --python .venv/bin/python -e ".[duckdb,polars,daft,tpcds_datagen,tpch_datagen,sparkmeasure,sail]"
              uv pip install --python .venv/bin/python pytest
            fi
            source .venv/bin/activate
          '';
        };
      }
    );
}
