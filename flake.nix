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
            update-venv() {
              rm -rf .venv
              uv venv --python ${python}/bin/python .venv
              source .venv/bin/activate

              uv pip install -e .
              for extra in duckdb polars daft tpcds_datagen tpch_datagen sparkmeasure sail; do
                uv pip install "lakebench[$extra]" 2>&1 || echo "warning: $extra failed to install, skipping"
              done
              uv pip install pytest jupyter ipykernel

              python -m ipykernel install --user --name python3 --display-name "Python 3 (LakeBench)"
            }

            if [ ! -d .venv ]; then
              update-venv
            else
              source .venv/bin/activate
            fi
          '';
        };
      }
    );
}
