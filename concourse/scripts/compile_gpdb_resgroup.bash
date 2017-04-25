#!/bin/bash -l
set -exo pipefail

GREENPLUM_INSTALL_DIR=/usr/local/greenplum-db-devel
export GPDB_ARTIFACTS_DIR
GPDB_ARTIFACTS_DIR=$(pwd)/$OUTPUT_ARTIFACT_DIR

CWDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

${CWDIR}/ic_gpdb.bash

pushd gpdb_src
version="$(git describe)"
popd

pushd $GREENPLUM_INSTALL_DIR
TARBALL="$GPDB_ARTIFACTS_DIR"/bin_gpdb.tar.gz
VERSIONED_TARBALL="$GPDB_ARTIFACTS_DIR"/bin_gpdb.${version}.tar.gz
ln -nf "$TARBALL" "$VERSIONED_TARBALL"
popd
