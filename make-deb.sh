#!/bin/bash -e

unset CDPATH

if [ -d debian ]; then
    rm -rf debian
fi

mkdir debian

VERSION="$(cat version)"

if ! (echo "$VERSION" | egrep -q '^[0-9]+\.[0-9]+$'); then
    echo "Invalid version number $VERSION" 1>&2
    exit 1
fi

if [ "$(hg branch)" = 'default' ]; then
    REVISION="$(hg id -n)"
    VERSION="${VERSION}-tip"
elif [ "$(hg branch)" = "$VERSION" ]; then
    REVISION="$(hg id -n)"
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
hg log --style=changelog | gzip -9 > debian/usr/share/doc/hanzo-warc-tools/changelog.gz

cat <<EOF | gzip -9 > debian/usr/share/doc/hanzo-warc-tools/changelog.Debian.gz
hanzo-warc-tools ($VERSION) Hanzo;

 * Made debian style package  

 -- Stephen Jones <stephen.jones@hanzoarchives.com> $(date +'%a, %d %h %Y %T %z')
EOF

cat <<EOF > debian/DEBIAN/postinst
#!/bin/bash -e

if which pycompile >/dev/null 2>&1; then
  pycompile -p hanzo-warc-tools
fi
EOF

pushd debian

find usr/bin -type f -name '*.py' | (
    while read SCRIPT; do
	mv "$SCRIPT" "${SCRIPT%.py}"
	chmod 755 "${SCRIPT%.py}"
    done
)
md5sum $(find . -path ./DEBIAN -prune -o -type f -print) > DEBIAN/md5sums

find usr/lib -type f -exec chmod 644 '{}' ';'
find usr/share -type f -exec chmod 644 '{}' ';'
find DEBIAN -type f -exec chmod 644 '{}' ';'
find . -type d -exec chmod 755 '{}' ';'

chmod 755 DEBIAN/postinst

popd

fakeroot dpkg-deb --build debian .

lintian "hanzo-warc-tools_${VERSION}-${REVISION}_all.deb"

if [ -n "$1" ] && [ -d "$1" ] && [ -w "$1" ]; then
    mv "hanzo-warc-tools_${VERSION}-${REVISION}_all.deb" "$1"
fi
