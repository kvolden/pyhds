"""
This is a very incomplete python library for downloading streams
in the Adobe HDS format, heavily inspired by K-S-V's AdobeHDS.php
(https://github.com/K-S-V/Scripts/blob/master/AdobeHDS.php).
It is a work in progress, as is my understanding of HDS streams
and the Adobe Flash Video file format, and a lot of this code will
change.

tl;dr: Move on and pay no attention to this mess.
"""

import requests
import struct
from bs4 import BeautifulSoup
try:
    from urllib.parse import urlparse, urljoin
except:
    from urlparse import urlparse, urljoin
#try:
    import base64

I64_SIZE = 8
I32_SIZE = 4
I24_SIZE = 3
I8_SIZE  = 1


AUDIO = 0x08
VIDEO = 0x09
SCRIPT_DATA = 0x12
FRAME_TYPE_INFO = 0x05
CODEC_ID_AVC = 0x07
CODEC_ID_AAC = 0x0A
AVC_SEQUENCE_HEADER = 0x00
AAC_SEQUENCE_HEADER = 0x00
AVC_NALU = 0x01
AVC_SEQUENCE_END = 0x02
FRAMEFIX_STEP = 40
INVALID_TIMESTAMP = -1
STOP_PROCESSING = 2


class F4F:
    #var $audio, $auth, $baseFilename, $baseTS, $bootstrapUrl, $baseUrl, $debug, $duration, $fileCount, $filesize, $fixWindow;
    #var $format, $live, $media, $metadata, $outDir, $outFile, $parallel, $play, $processed, $quality, $rename, $video;
    #var $prevTagSize, $tagHeaderLen;
    #var $segTable, $fragTable, $frags, $fragCount, $lastFrag, $fragUrl, $discontinuity;
    #var $negTS, $prevAudioTS, $prevVideoTS, $pAudioTagLen, $pVideoTagLen, $pAudioTagPos, $pVideoTagPos;
    #var $prevAVC_Header, $prevAAC_Header, $AVC_HeaderWritten, $AAC_HeaderWritten;

    def __init__(self):
        self.auth          = ""
        self.baseFilename  = ""
        self.bootstrapUrl  = ""
        self.debug         = False
        self.duration      = 0
        self.fileCount     = 1
        self.fixWindow     = 1000
        self.format        = ""
        self.live          = False
        self.metadata      = True
        self.outDir        = ""
        self.outFile       = ""
        self.parallel      = 8
        self.play          = False
        self.processed     = False
        self.quality       = "high"
        self.rename        = False
        self.segTable      = []
        self.fragTable     = []
        self.segStart      = False
        self.fragStart     = False
        self.frags         = []
        self.fragCount     = 0
        self.lastFrag      = 0
        self.discontinuity = ""
        self.media = [] # SHOULD PERHAPS NOT BE HERE
        self.init_decoder() #



    def init_decoder(self):
        self.audio              = False
        self.filesize           = 0
        self.video              = False
        self.prev_tag_size      = 4
        self.tag_header_len     = 11
        self.base_TS            = INVALID_TIMESTAMP
        self.neg_TS             = INVALID_TIMESTAMP
        self.prev_audio_TS      = INVALID_TIMESTAMP
        self.prev_video_TS      = INVALID_TIMESTAMP
        self.p_audio_tag_len    = 0
        self.p_video_tag_len    = 0
        self.p_audio_tag_pos    = 0
        self.p_video_tag_pos    = 0
        self.prev_AVC_header    = False
        self.prev_AAC_header    = False
        self.AVC_header_written = False
        self.AAC_header_written = False
        
    def get_manifest(self, manifest_url):
        req = requests.get(manifest_url)
        req.raise_for_status()
        xml_soup = BeautifulSoup(req.text, "xml")
        # FIXME: Error handling
        return xml_soup

    def parse_manifest(self, main_manifest_url, max_bitrate = float("inf")):
        main_soup = self.get_manifest(main_manifest_url)
    
        # First get the manifests containing the media
        media_manifests = []

        if main_soup.manifest.media.get("href"):    # Multi-level manifest, get stream-levels from set-level
            if main_soup.manifest.baseURL:
                base_url = main_soup.manifest.baseURL.string
            else:
                base_url = urljoin(main_manifest_url, ".")

            for media_tag in main_soup.manifest.find_all("media"):
                bitrate = int(media_tag.get("bitrate") or 0)    # NOTE: incrementing in original
                url = urljoin(base_url, media_tag.get("href"))
                child_soup = get_manifest(url)
                manifest = {"bitrate": bitrate, "url": url, "xml": child_soup}
                media_manifests.append(manifest)
    
        else:                                       # Single-level manifest
            manifest = {"bitrate": 0, "url": main_manifest_url, "xml": main_soup}
            media_manifests.append(manifest)

        # Then extract information from all media containing manifests
        for manifest in media_manifests:
            manifest_soup = manifest["xml"]
            if manifest_soup.manifest.baseURL:
                base_url = main_soup.manifest.baseURL.string
            else:
                base_url = urljoin(main_manifest_url, ".")

            for media_tag in manifest_soup.find_all("media"):
                media_entry = dict(media_tag.attrs)
                media_entry["metadata"] = base64.decodestring(media_tag.metadata.string)
                media_entry["base_url"] = base_url
                media_entry["bitrate"] = int(media_tag.get("bitrate") or manifest["bitrate"])

                if "?" in media_entry["url"]:
                    media_entry["url"], media_entry["query_string"] = media_entry["url"].split("?")  # NOTE: Keeps ? in original ALSO NOTE: query string for url in main manifest lost
                else:
                    media_entry["query_string"] = self.auth  # NOTE: Needed both here and in bootstrapUrl?

                if media_tag.get("bootstrap_info_id"):
                    bootstrap = manifest_soup.find("bootstrap_info", id = media_tag.get("bootstrap_info_id"))
                else:
                    bootstrap = manifest_soup.bootstrapInfo
                if bootstrap.get("url"):
                    media_entry["bootstrap_url"] = urljoin(base_url, bootstrap.get("url"))
                    if not "?" in media_entry["bootstrap_url"]:
                        media_entry["bootstrap_url"] += self.auth
                else:
                    media_entry["bootstrap"] = base64.decodestring(bootstrap.string)
                
                self.media.append(media_entry)

        #545
        #THIS CAN BE REMOVED:
        bitrates = []
        if len(self.media) == 0:
            print("No media entry found");
        print("Manifest Entries:\n")
        print(" {:->8}  {}".format("Bitrate", "URL"))
        for entry in self.media:
            bitrates.append(entry["bitrate"])
            print(" {:->8}  {}".format(entry["bitrate"], entry["url"]))
        
        print("")
        print("Quality Selection:\n Available: {}".format(bitrates));
        # / THIS CAN BE REMOVED
        
        stream = self.select_stream(self.media, max_bitrate)

        print("\nSelected: {} - {}".format(stream["bitrate"], stream["url"]))

        # update bootstrap info if not set

        header = self.read_box_header(stream["bootstrap"])
        assert(header["box_type"] == "abst")
        offset = header["header_size"]
        self.parse_abst_box(stream["bootstrap"][offset:])

        

    def select_stream(self, streams, max_bitrate = float("inf")):
        selected_bitrate = 0
        for stream in streams:
            if selected_bitrate < stream["bitrate"] <= max_bitrate:
                selected = stream
                selected_bitrate = selected["bitrate"]
        return selected
        

    def read_box_header(self, data):
        pattern = ">I 4s Q"  # 32b uint, 4c string, 64b uint, big endian
        unpacked = struct.unpack(pattern, data[:16])

        box_type = unpacked[1]
        box_size = max(0, unpacked[3] - 16) if unpacked[0] == 1 else max(0, unpacked[0] - 8)
        header_size = 16 if unpacked[0] == 1 else 8

        header = {"box_type": box_type, "box_size": box_size, "header_size": header_size}

        return header


    def update_bootstrap_info(self, bootstrap_url): # FIXME: Only defined
        return

        
    def parse_abst_box(self, data):     # Parse bootstrap info box
        offset = 8            # Skipping some unneeded stuff for now
        byte = self.read_UI8(data[offset:])

        live = bool(byte & 0x20)
        if live:
            self.live = True
            self.metadata = False

        update = bool(byte & 0x10)

        # skipping a bunch of unneeded stuff. do this properly and save data somehow, in case module user wants it
        offset += 21
        offset += len(self.read_string(data[offset:])) + 1

        server_entry_count = self.read_UI8(data[offset:])
        offset += I8_SIZE

        for i in range(server_entry_count):
            offset += len(self.read_string(data[offset:])) + 1

        quality_entry_count = self.read_UI8(data[offset:])
        offset += I8_SIZE
        for i in range(quality_entry_count + 2):
            offset += len(self.read_string(data[offset:])) + 1

        # Create segment table:
        segment_run_table_count = self.read_UI8(data[offset:])
        offset += I8_SIZE
        #seg_table = {}
        for i in range(segment_run_table_count):
            header = self.read_box_header(data[offset:])  #682
            offset += header["header_size"]
            if header["box_type"] == "asrt" and i == 0:
                first_seg_run_table = self.parse_asrt_box(data[offset:])
                # FIXME: if not update: /else
                self.seg_table = first_seg_run_table
            offset += header["box_size"]

        # Create fragment table:
        fragment_run_table_count = self.read_UI8(data[offset:])
        offset += I8_SIZE
        #frag_table = {}
        for i in range(fragment_run_table_count):
            header = self.read_box_header(data[offset:])
            offset += header["header_size"]
            if header["box_type"] == "afrt" and i == 0:
                first_frag_run_table = self.parse_afrt_box(data[offset:])
                # FIXME: if not update : /else
                self.frag_table = first_frag_run_table
            offset += header["box_size"]

        self.parse_seg_and_frag_table()


    def parse_asrt_box(self, data):     # Parse segment run table box
        offset = 4
        quality_entry_count = self.read_UI8(data[offset:])
        offset += I8_SIZE
        for i in range(quality_entry_count):
            offset += len(self.read_string(data[offset:])) + 1

        segment_run_entry_count = self.read_UI32(data[offset:])
        offset += I32_SIZE
        segment_run_entry_table = []
        for i in range(segment_run_entry_count):
            segment_run_entry = {}
            first_segment = self.read_UI32(data[offset:])
            offset += I32_SIZE
            segment_run_entry["first_segment"] = first_segment
            segment_run_entry["fragments_per_segment"] = max(0, self.read_I32(data[offset:])) # FIXME: why? spec says UI32
            offset += I32_SIZE
            segment_run_entry_table.append(segment_run_entry)
        
        return segment_run_entry_table

    
    def parse_afrt_box(self, data):     # Parse fragment run table box
        offset = 8
        quality_entry_count = self.read_UI8(data[offset:])
        offset += I8_SIZE
        for i in range(quality_entry_count):
            offset += len(self.read_string(data[offset:])) + 1

        fragment_run_entry_count = self.read_UI32(data[offset:])
        offset += I32_SIZE
        fragment_run_entry_table = []
        for i in range(fragment_run_entry_count):
            fragment_run_entry = {}
            first_fragment = self.read_UI32(data[offset:])
            offset += I32_SIZE
            fragment_run_entry["first_fragment"] = first_fragment
            fragment_run_entry["first_fragment_timestamp"] = self.read_UI64(data[offset:])
            offset += I64_SIZE
            fragment_run_entry["fragment_duration"] = self.read_UI32(data[offset:])
            offset += I32_SIZE
            if fragment_run_entry["fragment_duration"] == 0:
                fragment_run_entry["discontinuity_indicator"] = self.read_UI8(data[offset:])
                offset += I8_SIZE
            else:
                fragment_run_entry["discontinuity_indicator"] = ""
            
            fragment_run_entry_table.append(fragment_run_entry)
        
        return fragment_run_entry_table

    
    def parse_seg_and_frag_table(self):
        # set self.{live, fragcount, segstart, fragstart}

        if self.frag_table[-1]["fragment_duration"] == 0 and self.frag_table[-1]["discontinuity_indicator"] == 0:
            self.live = False
            self.frag_table.pop()

        
        return


    def read_UI64(self, data):
        return struct.unpack(">Q", data[:I64_SIZE])[0]

    
    def read_I32(self, data):
        return struct.unpack(">i", data[:I32_SIZE])[0]

    
    def read_UI32(self, data):
        return struct.unpack(">I", data[:I32_SIZE])[0]

    
    def read_UI8(self, data):
        return struct.unpack("B", data[:I8_SIZE])[0]

    
    def read_string(self, data):  # Reads from data as string until it hits 0x00
        string = ""
        for char in data:
            if char == "\0":
                break
            else:
                string += char
        return string


# Just for testing:
if __name__ == '__main__':
    obj = F4F()
    obj.parse_manifest("http://nrkclip1c-f.akamaihd.net/z/wo/open/bc/bc0f6cfdac34bfc9ee0f210dd3b082f69c77e3c5/bc0f6cfdac34bfc9ee0f210dd3b082f69c77e3c5_,141,316,563,1266,2250,.mp4.csmil/manifest.f4m?g=NCCKJHTCTCRL&hdcore=3.5.0&plugin=aasp-3.5.0.151.81")

