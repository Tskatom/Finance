import os
import shutil
import urllib2
import urlparse


def download(url, fileName=None):
    def getFileName(url, openUrl):
        if 'Content-Disposition' in openUrl.info():
            cd = dict(map(
                lambda x: x.strip().split('=') if '=' in x else
                (x.strip(), ''),
                openUrl.info()['Content-Disposition'].split(';')))
            if 'filename' in cd:
                filename = cd['filename'].strip("\"'")
                if filename:
                    return filename
        # if no filename was found above, parse it out of the final URL.
        return os.path.basename(urlparse.urlsplit(openUrl.url)[2])

    r = urllib2.urlopen(urllib2.Request(url))
    try:
        fileName = fileName or getFileName(url, r)
        with open(fileName, 'wb') as f:
            shutil.copyfileobj(r, f)
    finally:
        r.close()


def main():
    url = "http://www.bvc.com.co/mercados/\
    DescargaXlsServlet?archivo=indices&codI\
    ndice=ICAP&fechaMin=20130424&fechaMax=20130424"
    download(url, 'index.xls')


if __name__ == "__main__":
    main()
