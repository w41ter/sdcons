#!/bin/bash

mkdir -p target/cov/
filename="target/cov/sdcons"

RUSTFLAGS="-Z instrument-coverage" \
    LLVM_PROFILE_FILE="${filename}-%m.profraw" \
    cargo test --tests --verbose

rustup default | grep nightly > /dev/null
if [[ $? == "0" ]]; then
    echo "try report coverage"
    cargo profdata -- merge \
        -sparse ${filename}-*.profraw \
        -o ${filename}.profdata
    cargo cov -- report \
        --use-color \
        --ignore-filename-regex='/rustc/' \
        --ignore-filename-regex='/.cargo/registry' \
        $( \
            for file in \
                $( \
                  RUSTFLAGS="-Z instrument-coverage" \
                    cargo test --tests --no-run --message-format=json \
                      | jq -r "select(.profile.test == true) | .filenames[]" \
                      | grep -v dSYM - \
                ); \
            do \
                printf "%s %s " -object $file; \
            done \
        ) \
        --instr-profile=${filename}.profdata --summary-only
fi

