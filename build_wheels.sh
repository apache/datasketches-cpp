#!/bin/bash
set -e -u -x

PLAT=manylinux2010_x86_64

function repair_wheel {
    wheel="$1"
    if ! auditwheel show "$wheel"; then
        echo "Skipping non-platform wheel $wheel"
    else
        auditwheel repair "$wheel" --plat "$PLAT" -w dist/
    fi
}

# Compile wheels
for PYBIN in /opt/python/*/bin; do
    "${PYBIN}/pip" install wheel
    "${PYBIN}/pip" wheel . --no-deps -w dist/
done

# Bundle external shared libraries into the wheels
for whl in dist/*.whl; do
    repair_wheel "$whl"
done
