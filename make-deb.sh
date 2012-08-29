#!/bin/bash -e

unset CDPATH

if [ -d debian ]; then
    rm -rf debian
fi

mkdir debian

if [ "$(hg branch)" = 'default' ]; then
    VERSION="$(hg log | awk '/^branch:/ { print $2 }' | head -n 1)"
    CHANGESET="$(hg log -b "$VERSION" | awk '/^changeset:/ { gsub("^[0-9]+:", "", $2); print $2 }' | tail -n 1)"
    REVISION="$(hg log -b default -r "${CHANGESET}:" | grep '^changeset:' | wc -l)"
    VERSION="${VERSION}-tip"
else
    VERSION="$(hg branch)"
    CHANGESET="$(hg log -b "$VERSION" | awk '/^changeset:/ { gsub("^[0-9]+:", "", $2); print $2 }' | tail -n 1)"
    REVISION="$(hg log -b "$VERSION" | grep '^changeset:' | wc -l)"
fi


mkdir -p debian/DEBIAN
cat <<EOF > debian/DEBIAN/control
Package: hanzo-warc-tools
Version: ${VERSION}-${REVISION}
Maintainer: Stephen Jones <stephen.jones@hanzoarchives.com>
Section: admin
Priority: optional
Architecture: all
Depends: python (>= 2.7)
Description: Suite of tools and libraries for manipulating warc files.
 Provides commands for listing the contents of warc files and libraries for
 manipulating warc files and http.
EOF

python setup.py install -q --no-compile --root "$PWD/debian" --install-layout=deb

mkdir -p debian/usr/share/doc/hanzo-warc-tools
echo "Copyright Hanzo Archives $(date +%Y)" > debian/usr/share/doc/hanzo-warc-tools/copyright
cp README debian/usr/share/doc/hanzo-warc-tools/
if [ "$(hg branch)" = 'default' ]; then
    hg log -b default --style=changelog | gzip -9 > debian/usr/share/doc/hanzo-warc-tools/changelog.gz
else
    hg log -b "$VERSION" --style=changelog && hg log -b default -r ":${CHANGESET}" | gzip -9 > debian/usr/share/doc/hanzo-warc-tools/changelog
fi

cat <<EOF | gzip -9 > debian/usr/share/doc/hanzo-warc-tools/changelog.Debian.gz
hanzo-warc-tools (all) Hanzo
  * Made debian style package  

  -- Stephen Jones <stephen.jones@hanzoarchives.com>
EOF

pushd debian

find usr/bin -type f -name '*.py' | (
    while read SCRIPT; do
	mv "$SCRIPT" "${SCRIPT%.py}"
	chmod 755 "${SCRIPT%.py}"
    done
)
find usr/lib -type f -exec chmod 644 '{}' ';'

md5sum $(find . -path ./DEBIAN -prune -o -type f -print) > DEBIAN/md5sums

popd


fakeroot dpkg-deb --build debian .

lintian "hanzo-warc-tools_${VERSION}-${REVISION}_all.deb"
