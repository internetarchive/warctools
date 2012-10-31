#!/usr/bin/python
import os
import re
import sys
import os.path
import logging

from urlparse import urlparse, urlunparse
from HTMLParser import HTMLParser, HTMLParseError
from optparse import OptionParser
from contextlib import closing

from .warctools import WarcRecord, expand_files
from .httptools import RequestMessage, ResponseMessage


LEVELS = {'debug': logging.DEBUG,
          'info': logging.INFO,
          'warning': logging.WARNING,
          'error': logging.ERROR,
          'critical': logging.CRITICAL}

parser = OptionParser(usage="%prog [options] warc (warc ...)")

parser.add_option("-L", "--log-level", dest="log_level")

parser.set_defaults(log_level="info")



def parse_http_response(record):
    message = ResponseMessage(RequestMessage())
    remainder = message.feed(record.content[1])
    message.close()
    if remainder or not message.complete():
        if remainder:
            logging.warning('trailing data in http response for %s'% record.url)
        if not message.complete():
            logging.warning('truncated http response for %s'%record.url)

    header = message.header

    mime_type = [v for k,v in header.headers if k.lower() =='content-type']
    if mime_type:
        mime_type = mime_type[0].split(';')[0]
    else:
        mime_type = None

    return header.code, mime_type, message


def extract_links_from_warcfh(fh):
    for (offset, record, errors) in fh.read_records(limit=None):
        if record:
            try:
                content_type, content = record.content

                if record.type == WarcRecord.RESPONSE and content_type.startswith('application/http'):

                    code, mime_type, message = parse_http_response(record)

                    if 200 <= code < 300 and mime_type.find('html') > -1: 
                        for link in extract_links_from_html(record.url, message.get_body()):
                            yield ("".join(c for c in link if c not in '\n\r\t'))


            except StandardError, e:
                logging.warning("error in handling record "+str(e))
                import traceback; traceback.print_exc()

        elif errors:
            logging.warning("warc error at %d: %s"%((offset if offset else 0), ", ".join(str(e) for e in errors)))
            import traceback; traceback.print_exc()



try:
    import lxml.html

    def extract_links_from_html(base, body):
        try:
            html = lxml.html.fromstring(body)
            html.make_links_absolute(base)

            for element, attribute, link, pos in html.iterlinks():
                if isinstance(link, unicode):
                    link = link.encode('utf-8', 'ignore')
                yield link

        except StandardError:
            logging.warning("(lxml) html parse error")
            import traceback; traceback.print_exc()
            

except ImportError:
    logging.warning("using fallback parser")
    def extract_links_from_html(base, body):
        try:
            html = LinkParser(base)
            html.feed(body)
            html.close()
            for link in html.get_abs_links():
                yield link
        except HTMLParseError,ex:
            logging.warning("html parse error")


""" fallback link extractor """
def attr_extractor(*names):
        def _extractor(attrs):
            return [value for key,value in attrs if key in names and value]
        return _extractor

def meta_extractor(attrs):
    content = [value for key,value in attrs if key =="content" and value]
    urls = []
    for value in content:
        for pair in value.split(";"):
            bits = pair.split("=",2)
            if len(bits)>1 and bits[0].lower()=="url":
                urls.append(bits[1].strip())
    return urls


class LinkParser(HTMLParser):
    def __init__(self, base):
        HTMLParser.__init__(self)
        self.links = []
        self.base = base

        self.tag_extractor = {
            "a": attr_extractor("href"),
            "applet": attr_extractor("code"),
            "area": attr_extractor("href"),
            "bgsound": attr_extractor("src"),
            "body": attr_extractor("background"),
            "embed": attr_extractor("href","src"),
            "fig": attr_extractor("src"),
            "form": attr_extractor("action"),
            "frame": attr_extractor("src"),
            "iframe": attr_extractor("src"),
            "img": attr_extractor("href","src","lowsrc"),
            "input": attr_extractor("src"),
            "link": attr_extractor("href"),
            "layer": attr_extractor("src"),
            "object": attr_extractor("data"),
            "overlay": attr_extractor("src"),
            "script": attr_extractor("src"),
            "table": attr_extractor("background"),
            "td": attr_extractor("background"),
            "th": attr_extractor("background"),

            "meta": meta_extractor,
            "base": self.base_extractor,
        }

    def base_extractor(self, attrs):
        base = [value for key,value in attrs if key == "href" and value]
        if base:
            self.base = base[-1]
        return ()

    def handle_starttag(self, tag, attrs):
        extractor = self.tag_extractor.get(tag, None)
        if extractor:
            self.links.extend(extractor(attrs))

    def get_abs_links(self):
        full_urls = []
        root = urlparse(self.base)
        root_dir = os.path.split(root.path)[0]
        for link in self.links:
            parsed = urlparse(link)
            if not parsed.netloc: # does it have no protocol or host, i.e relative
                if parsed.path.startswith("/"):
                    parsed = root[0:2] + parsed[2:5] + (None,)
                else:
                    dir = root_dir
                    path = parsed.path
                    while True:
                        if path.startswith("../"):
                            path=path[3:]
                            dir=os.path.split(dir)[0]
                        elif path.startswith("./"):
                            path=path[2:]
                        else:
                            break

                    parsed = root[0:2] + (os.path.join(dir, path),) + parsed[3:5] + (None,)
                new_link = urlunparse(parsed)
                logging.debug("relative %s -> %s"%(link, new_link))
                link=new_link

            else:
                logging.debug("absolute %s"%link)
            full_urls.append(link)
        return full_urls


def main(argv):
    (options, warcs) = parser.parse_args(args=argv[1:])
    logging.basicConfig(level=LEVELS[options.log_level])

    if len(warcs) < 1:
        parser.error("missing warcs(s)")
        

    ret = 0

    for warc in expand_files(warcs):
        try:
            with closing(WarcRecord.open_archive(filename=warc, gzip="auto")) as fh:
                for link in extract_links_from_warcfh(fh):
                    print link

        except StandardError as e:
            logging.error(str(e))
            ret -=1

    return ret


def run():
    sys.exit(main(sys.argv))


if __name__ == '__main__':  
    run()


