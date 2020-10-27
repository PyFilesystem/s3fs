from __future__ import absolute_import
from __future__ import unicode_literals
import os

import unittest

from nose.plugins.attrib import attr

from fs.test import FSTestCases
from fs_s3fs import S3FS, S3FSZero

import boto3

from datetime import datetime
import io
import itertools
import json
import math
import os
import time
import unittest

import fs.copy
import fs.move
from fs import ResourceType, Seek
from fs import errors
from fs import walk
from fs import glob
from fs.opener import open_fs
from fs.subfs import ClosingSubFS, SubFS

import pytz
import six
from six import text_type

if six.PY2:
    import collections as collections_abc
else:
    import collections.abc as collections_abc


UNICODE_TEXT = """
UTF-8 encoded sample plain-text file
‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾
Markus Kuhn [ˈmaʳkʊs kuːn] &lt;mkuhn@acm.org> — 1999-08-20
The ASCII compatible UTF-8 encoding of ISO 10646 and Unicode
plain-text files is defined in RFC 2279 and in ISO 10646-1 Annex R.
Using Unicode/UTF-8, you can write in emails and source code things such as
Mathematics and Sciences:
  ∮ E⋅da = Q,  n → ∞, ∑ f(i) = ∏ g(i), ∀x∈ℝ: ⌈x⌉ = −⌊−x⌋, α ∧ ¬β = ¬(¬α ∨ β),
  ℕ ⊆ ℕ₀ ⊂ ℤ ⊂ ℚ ⊂ ℝ ⊂ ℂ, ⊥ &lt; a ≠ b ≡ c ≤ d ≪ ⊤ ⇒ (A ⇔ B),
  2H₂ + O₂ ⇌ 2H₂O, R = 4.7 kΩ, ⌀ 200 mm
Linguistics and dictionaries:
  ði ıntəˈnæʃənəl fəˈnɛtık əsoʊsiˈeıʃn
  Y [ˈʏpsilɔn], Yen [jɛn], Yoga [ˈjoːgɑ]
APL:
  ((V⍳V)=⍳⍴V)/V←,V    ⌷←⍳→⍴∆∇⊃‾⍎⍕⌈
Nicer typography in plain text files:
  ╔══════════════════════════════════════════╗
  ║                                          ║
  ║   • ‘single’ and “double” quotes         ║
  ║                                          ║
  ║   • Curly apostrophes: “We’ve been here” ║
  ║                                          ║
  ║   • Latin-1 apostrophe and accents: '´`  ║
  ║                                          ║
  ║   • ‚deutsche‘ „Anführungszeichen“       ║
  ║                                          ║
  ║   • †, ‡, ‰, •, 3–4, —, −5/+5, ™, …      ║
  ║                                          ║
  ║   • ASCII safety test: 1lI|, 0OD, 8B     ║
  ║                      ╭─────────╮         ║
  ║   • the euro symbol: │ 14.95 € │         ║
  ║                      ╰─────────╯         ║
  ╚══════════════════════════════════════════╝
Greek (in Polytonic):
  The Greek anthem:
  Σὲ γνωρίζω ἀπὸ τὴν κόψη
  τοῦ σπαθιοῦ τὴν τρομερή,
  σὲ γνωρίζω ἀπὸ τὴν ὄψη
  ποὺ μὲ βία μετράει τὴ γῆ.
  ᾿Απ᾿ τὰ κόκκαλα βγαλμένη
  τῶν ῾Ελλήνων τὰ ἱερά
  καὶ σὰν πρῶτα ἀνδρειωμένη
  χαῖρε, ὦ χαῖρε, ᾿Ελευθεριά!
  From a speech of Demosthenes in the 4th century BC:
  Οὐχὶ ταὐτὰ παρίσταταί μοι γιγνώσκειν, ὦ ἄνδρες ᾿Αθηναῖοι,
  ὅταν τ᾿ εἰς τὰ πράγματα ἀποβλέψω καὶ ὅταν πρὸς τοὺς
  λόγους οὓς ἀκούω· τοὺς μὲν γὰρ λόγους περὶ τοῦ
  τιμωρήσασθαι Φίλιππον ὁρῶ γιγνομένους, τὰ δὲ πράγματ᾿
  εἰς τοῦτο προήκοντα,  ὥσθ᾿ ὅπως μὴ πεισόμεθ᾿ αὐτοὶ
  πρότερον κακῶς σκέψασθαι δέον. οὐδέν οὖν ἄλλο μοι δοκοῦσιν
  οἱ τὰ τοιαῦτα λέγοντες ἢ τὴν ὑπόθεσιν, περὶ ἧς βουλεύεσθαι,
  οὐχὶ τὴν οὖσαν παριστάντες ὑμῖν ἁμαρτάνειν. ἐγὼ δέ, ὅτι μέν
  ποτ᾿ ἐξῆν τῇ πόλει καὶ τὰ αὑτῆς ἔχειν ἀσφαλῶς καὶ Φίλιππον
  τιμωρήσασθαι, καὶ μάλ᾿ ἀκριβῶς οἶδα· ἐπ᾿ ἐμοῦ γάρ, οὐ πάλαι
  γέγονεν ταῦτ᾿ ἀμφότερα· νῦν μέντοι πέπεισμαι τοῦθ᾿ ἱκανὸν
  προλαβεῖν ἡμῖν εἶναι τὴν πρώτην, ὅπως τοὺς συμμάχους
  σώσομεν. ἐὰν γὰρ τοῦτο βεβαίως ὑπάρξῃ, τότε καὶ περὶ τοῦ
  τίνα τιμωρήσεταί τις καὶ ὃν τρόπον ἐξέσται σκοπεῖν· πρὶν δὲ
  τὴν ἀρχὴν ὀρθῶς ὑποθέσθαι, μάταιον ἡγοῦμαι περὶ τῆς
  τελευτῆς ὁντινοῦν ποιεῖσθαι λόγον.
  Δημοσθένους, Γ´ ᾿Ολυνθιακὸς
Georgian:
  From a Unicode conference invitation:
  გთხოვთ ახლავე გაიაროთ რეგისტრაცია Unicode-ის მეათე საერთაშორისო
  კონფერენციაზე დასასწრებად, რომელიც გაიმართება 10-12 მარტს,
  ქ. მაინცში, გერმანიაში. კონფერენცია შეჰკრებს ერთად მსოფლიოს
  ექსპერტებს ისეთ დარგებში როგორიცაა ინტერნეტი და Unicode-ი,
  ინტერნაციონალიზაცია და ლოკალიზაცია, Unicode-ის გამოყენება
  ოპერაციულ სისტემებსა, და გამოყენებით პროგრამებში, შრიფტებში,
  ტექსტების დამუშავებასა და მრავალენოვან კომპიუტერულ სისტემებში.
Russian:
  From a Unicode conference invitation:
  Зарегистрируйтесь сейчас на Десятую Международную Конференцию по
  Unicode, которая состоится 10-12 марта 1997 года в Майнце в Германии.
  Конференция соберет широкий круг экспертов по  вопросам глобального
  Интернета и Unicode, локализации и интернационализации, воплощению и
  применению Unicode в различных операционных системах и программных
  приложениях, шрифтах, верстке и многоязычных компьютерных системах.
Thai (UCS Level 2):
  Excerpt from a poetry on The Romance of The Three Kingdoms (a Chinese
  classic 'San Gua'):
  [----------------------------|------------------------]
    ๏ แผ่นดินฮั่นเสื่อมโทรมแสนสังเวช  พระปกเกศกองบู๊กู้ขึ้นใหม่
  สิบสองกษัตริย์ก่อนหน้าแลถัดไป       สององค์ไซร้โง่เขลาเบาปัญญา
    ทรงนับถือขันทีเป็นที่พึ่ง           บ้านเมืองจึงวิปริตเป็นนักหนา
  โฮจิ๋นเรียกทัพทั่วหัวเมืองมา         หมายจะฆ่ามดชั่วตัวสำคัญ
    เหมือนขับไสไล่เสือจากเคหา      รับหมาป่าเข้ามาเลยอาสัญ
  ฝ่ายอ้องอุ้นยุแยกให้แตกกัน          ใช้สาวนั้นเป็นชนวนชื่นชวนใจ
    พลันลิฉุยกุยกีกลับก่อเหตุ          ช่างอาเพศจริงหนาฟ้าร้องไห้
  ต้องรบราฆ่าฟันจนบรรลัย           ฤๅหาใครค้ำชูกู้บรรลังก์ ฯ
  (The above is a two-column text. If combining characters are handled
  correctly, the lines of the second column should be aligned with the
  | character above.)
Ethiopian:
  Proverbs in the Amharic language:
  ሰማይ አይታረስ ንጉሥ አይከሰስ።
  ብላ ካለኝ እንደአባቴ በቆመጠኝ።
  ጌጥ ያለቤቱ ቁምጥና ነው።
  ደሀ በሕልሙ ቅቤ ባይጠጣ ንጣት በገደለው።
  የአፍ ወለምታ በቅቤ አይታሽም።
  አይጥ በበላ ዳዋ ተመታ።
  ሲተረጉሙ ይደረግሙ።
  ቀስ በቀስ፥ ዕንቁላል በእግሩ ይሄዳል።
  ድር ቢያብር አንበሳ ያስር።
  ሰው እንደቤቱ እንጅ እንደ ጉረቤቱ አይተዳደርም።
  እግዜር የከፈተውን ጉሮሮ ሳይዘጋው አይድርም።
  የጎረቤት ሌባ፥ ቢያዩት ይስቅ ባያዩት ያጠልቅ።
  ሥራ ከመፍታት ልጄን ላፋታት።
  ዓባይ ማደሪያ የለው፥ ግንድ ይዞ ይዞራል።
  የእስላም አገሩ መካ የአሞራ አገሩ ዋርካ።
  ተንጋሎ ቢተፉ ተመልሶ ባፉ።
  ወዳጅህ ማር ቢሆን ጨርስህ አትላሰው።
  እግርህን በፍራሽህ ልክ ዘርጋ።
Runes:
  ᚻᛖ ᚳᚹᚫᚦ ᚦᚫᛏ ᚻᛖ ᛒᚢᛞᛖ ᚩᚾ ᚦᚫᛗ ᛚᚪᚾᛞᛖ ᚾᚩᚱᚦᚹᛖᚪᚱᛞᚢᛗ ᚹᛁᚦ ᚦᚪ ᚹᛖᛥᚫ
  (Old English, which transcribed into Latin reads 'He cwaeth that he
  bude thaem lande northweardum with tha Westsae.' and means 'He said
  that he lived in the northern land near the Western Sea.')
Braille:
  ⡌⠁⠧⠑ ⠼⠁⠒  ⡍⠜⠇⠑⠹⠰⠎ ⡣⠕⠌
  ⡍⠜⠇⠑⠹ ⠺⠁⠎ ⠙⠑⠁⠙⠒ ⠞⠕ ⠃⠑⠛⠔ ⠺⠊⠹⠲ ⡹⠻⠑ ⠊⠎ ⠝⠕ ⠙⠳⠃⠞
  ⠱⠁⠞⠑⠧⠻ ⠁⠃⠳⠞ ⠹⠁⠞⠲ ⡹⠑ ⠗⠑⠛⠊⠌⠻ ⠕⠋ ⠙⠊⠎ ⠃⠥⠗⠊⠁⠇ ⠺⠁⠎
  ⠎⠊⠛⠝⠫ ⠃⠹ ⠹⠑ ⠊⠇⠻⠛⠹⠍⠁⠝⠂ ⠹⠑ ⠊⠇⠻⠅⠂ ⠹⠑ ⠥⠝⠙⠻⠞⠁⠅⠻⠂
  ⠁⠝⠙ ⠹⠑ ⠡⠊⠑⠋ ⠍⠳⠗⠝⠻⠲ ⡎⠊⠗⠕⠕⠛⠑ ⠎⠊⠛⠝⠫ ⠊⠞⠲ ⡁⠝⠙
  ⡎⠊⠗⠕⠕⠛⠑⠰⠎ ⠝⠁⠍⠑ ⠺⠁⠎ ⠛⠕⠕⠙ ⠥⠏⠕⠝ ⠰⡡⠁⠝⠛⠑⠂ ⠋⠕⠗ ⠁⠝⠹⠹⠔⠛ ⠙⠑
  ⠡⠕⠎⠑ ⠞⠕ ⠏⠥⠞ ⠙⠊⠎ ⠙⠁⠝⠙ ⠞⠕⠲
  ⡕⠇⠙ ⡍⠜⠇⠑⠹ ⠺⠁⠎ ⠁⠎ ⠙⠑⠁⠙ ⠁⠎ ⠁ ⠙⠕⠕⠗⠤⠝⠁⠊⠇⠲
  ⡍⠔⠙⠖ ⡊ ⠙⠕⠝⠰⠞ ⠍⠑⠁⠝ ⠞⠕ ⠎⠁⠹ ⠹⠁⠞ ⡊ ⠅⠝⠪⠂ ⠕⠋ ⠍⠹
  ⠪⠝ ⠅⠝⠪⠇⠫⠛⠑⠂ ⠱⠁⠞ ⠹⠻⠑ ⠊⠎ ⠏⠜⠞⠊⠊⠥⠇⠜⠇⠹ ⠙⠑⠁⠙ ⠁⠃⠳⠞
  ⠁ ⠙⠕⠕⠗⠤⠝⠁⠊⠇⠲ ⡊ ⠍⠊⠣⠞ ⠙⠁⠧⠑ ⠃⠑⠲ ⠔⠊⠇⠔⠫⠂ ⠍⠹⠎⠑⠇⠋⠂ ⠞⠕
  ⠗⠑⠛⠜⠙ ⠁ ⠊⠕⠋⠋⠔⠤⠝⠁⠊⠇ ⠁⠎ ⠹⠑ ⠙⠑⠁⠙⠑⠌ ⠏⠊⠑⠊⠑ ⠕⠋ ⠊⠗⠕⠝⠍⠕⠝⠛⠻⠹
  ⠔ ⠹⠑ ⠞⠗⠁⠙⠑⠲ ⡃⠥⠞ ⠹⠑ ⠺⠊⠎⠙⠕⠍ ⠕⠋ ⠳⠗ ⠁⠝⠊⠑⠌⠕⠗⠎
  ⠊⠎ ⠔ ⠹⠑ ⠎⠊⠍⠊⠇⠑⠆ ⠁⠝⠙ ⠍⠹ ⠥⠝⠙⠁⠇⠇⠪⠫ ⠙⠁⠝⠙⠎
  ⠩⠁⠇⠇ ⠝⠕⠞ ⠙⠊⠌⠥⠗⠃ ⠊⠞⠂ ⠕⠗ ⠹⠑ ⡊⠳⠝⠞⠗⠹⠰⠎ ⠙⠕⠝⠑ ⠋⠕⠗⠲ ⡹⠳
  ⠺⠊⠇⠇ ⠹⠻⠑⠋⠕⠗⠑ ⠏⠻⠍⠊⠞ ⠍⠑ ⠞⠕ ⠗⠑⠏⠑⠁⠞⠂ ⠑⠍⠏⠙⠁⠞⠊⠊⠁⠇⠇⠹⠂ ⠹⠁⠞
  ⡍⠜⠇⠑⠹ ⠺⠁⠎ ⠁⠎ ⠙⠑⠁⠙ ⠁⠎ ⠁ ⠙⠕⠕⠗⠤⠝⠁⠊⠇⠲
  (The first couple of paragraphs of "A Christmas Carol" by Dickens)
Compact font selection example text:
  ABCDEFGHIJKLMNOPQRSTUVWXYZ /0123456789
  abcdefghijklmnopqrstuvwxyz £©µÀÆÖÞßéöÿ
  –—‘“”„†•…‰™œŠŸž€ ΑΒΓΔΩαβγδω АБВГДабвгд
  ∀∂∈ℝ∧∪≡∞ ↑↗↨↻⇣ ┐┼╔╘░►☺♀ ﬁ�⑀₂ἠḂӥẄɐː⍎אԱა
Greetings in various languages:
  Hello world, Καλημέρα κόσμε, コンニチハ
Box drawing alignment tests:                                          █
                                                                      ▉
  ╔══╦══╗  ┌──┬──┐  ╭──┬──╮  ╭──┬──╮  ┏━━┳━━┓  ┎┒┏┑   ╷  ╻ ┏┯┓ ┌┰┐    ▊ ╱╲╱╲╳╳╳
  ║┌─╨─┐║  │╔═╧═╗│  │╒═╪═╕│  │╓─╁─╖│  ┃┌─╂─┐┃  ┗╃╄┙  ╶┼╴╺╋╸┠┼┨ ┝╋┥    ▋ ╲╱╲╱╳╳╳
  ║│╲ ╱│║  │║   ║│  ││ │ ││  │║ ┃ ║│  ┃│ ╿ │┃  ┍╅╆┓   ╵  ╹ ┗┷┛ └┸┘    ▌ ╱╲╱╲╳╳╳
  ╠╡ ╳ ╞╣  ├╢   ╟┤  ├┼─┼─┼┤  ├╫─╂─╫┤  ┣┿╾┼╼┿┫  ┕┛┖┚     ┌┄┄┐ ╎ ┏┅┅┓ ┋ ▍ ╲╱╲╱╳╳╳
  ║│╱ ╲│║  │║   ║│  ││ │ ││  │║ ┃ ║│  ┃│ ╽ │┃  ░░▒▒▓▓██ ┊  ┆ ╎ ╏  ┇ ┋ ▎
  ║└─╥─┘║  │╚═╤═╝│  │╘═╪═╛│  │╙─╀─╜│  ┃└─╂─┘┃  ░░▒▒▓▓██ ┊  ┆ ╎ ╏  ┇ ┋ ▏
  ╚══╩══╝  └──┴──┘  ╰──┴──╯  ╰──┴──╯  ┗━━┻━━┛           └╌╌┘ ╎ ┗╍╍┛ ┋  ▁▂▃▄▅▆▇█
"""

class FSTestCasesWithPrefix(object):
    """Basic FS tests.
    """
    prefix1=''
    prefix2=''

    def make_fs(self):
        """Return an FS instance.
        """
        raise NotImplementedError("implement me")

    def destroy_fs(self, fs):
        """Destroy a FS instance.
        Arguments:
            fs (FS): A filesystem instance previously opened
                by `~fs.test.FSTestCases.make_fs`.
        """
        fs.close()

    def setUp(self):
        self.fs = self.make_fs()

    def tearDown(self):
        self.destroy_fs(self.fs)
        del self.fs

    def assert_exists(self, path):
        """Assert a path exists.
        Arguments:
            path (str): A path on the filesystem.
        """
        self.assertTrue(self.fs.exists(self.prefix1+path))

    def assert_not_exists(self, path):
        """Assert a path does not exist.
        Arguments:
            path (str): A path on the filesystem.
        """
        self.assertFalse(self.fs.exists(self.prefix1+path))

    def assert_isfile(self, path):
        """Assert a path is a file.
        Arguments:
            path (str): A path on the filesystem.
        """
        self.assertTrue(self.fs.isfile(self.prefix1+path))

    def assert_isdir(self, path):
        """Assert a path is a directory.
        Arguments:
            path (str): A path on the filesystem.
        """
        self.assertTrue(self.fs.isdir(self.prefix1+path))

    def assert_bytes(self, path, contents):
        """Assert a file contains the given bytes.
        Arguments:
            path (str): A path on the filesystem.
            contents (bytes): Bytes to compare.
        """
        assert isinstance(contents, bytes)
        data = self.fs.readbytes(self.prefix1+path)
        self.assertEqual(data, contents)
        self.assertIsInstance(data, bytes)

    def assert_text(self, path, contents):
        """Assert a file contains the given text.
        Arguments:
            path (str): A path on the filesystem.
            contents (str): Text to compare.
        """
        assert isinstance(contents, text_type)
        with self.fs.open(self.prefix1+path, "rt") as f:
            data = f.read()
        self.assertEqual(data, contents)
        self.assertIsInstance(data, text_type)

    def test_root_dir(self):
        with self.assertRaises(errors.FileExpected):
            self.fs.open(self.prefix1+"/")
        with self.assertRaises(errors.FileExpected):
            self.fs.openbin(self.prefix1+"/")

    def test_appendbytes(self):
        with self.assertRaises(TypeError):
            self.fs.appendbytes(self.prefix1+"foo", "bar")
        self.fs.appendbytes(self.prefix1+"foo", b"bar")
        self.assert_bytes("foo", b"bar")
        self.fs.appendbytes(self.prefix1+"foo", b"baz")
        self.assert_bytes("foo", b"barbaz")

    def test_appendtext(self):
        with self.assertRaises(TypeError):
            self.fs.appendtext(self.prefix1+"foo", b"bar")
        self.fs.appendtext(self.prefix1+"foo", "bar")
        self.assert_text("foo", "bar")
        self.fs.appendtext(self.prefix1+"foo", "baz")
        self.assert_text("foo", "barbaz")

    def test_basic(self):
        #  Check str and repr don't break
        repr(self.fs)
        self.assertIsInstance(six.text_type(self.fs), six.text_type)

    def test_getmeta(self):
        # Get the meta dict
        meta = self.fs.getmeta()

        # Check default namespace
        self.assertEqual(meta, self.fs.getmeta(namespace="standard"))

        # Must be a dict
        self.assertTrue(isinstance(meta, dict))

        no_meta = self.fs.getmeta("__nosuchnamespace__")
        self.assertIsInstance(no_meta, dict)
        self.assertFalse(no_meta)

    def test_isfile(self):
        self.assertFalse(self.fs.isfile(self.prefix1+"foo.txt"))
        self.fs.create(self.prefix1+"foo.txt")
        self.assertTrue(self.fs.isfile(self.prefix1+"foo.txt"))
        self.fs.makedir(self.prefix1+"bar")
        self.assertFalse(self.fs.isfile(self.prefix1+"bar"))

    def test_isdir(self):
        self.assertFalse(self.fs.isdir(self.prefix1+"foo"))
        self.fs.create(self.prefix1+"bar")
        self.fs.makedir(self.prefix1+"foo")
        self.assertTrue(self.fs.isdir(self.prefix1+"foo"))
        self.assertFalse(self.fs.isdir(self.prefix1+"bar"))

    def test_islink(self):
        self.fs.touch(self.prefix1+"foo")
        self.assertFalse(self.fs.islink(self.prefix1+"foo"))
        with self.assertRaises(errors.ResourceNotFound):
            self.fs.islink(self.prefix1+"bar")

    def test_getsize(self):
        self.fs.writebytes(self.prefix1+"empty", b"")
        self.fs.writebytes(self.prefix1+"one", b"a")
        self.fs.writebytes(self.prefix1+"onethousand", ("b" * 1000).encode("ascii"))
        self.assertEqual(self.fs.getsize(self.prefix1+"empty"), 0)
        self.assertEqual(self.fs.getsize(self.prefix1+"one"), 1)
        self.assertEqual(self.fs.getsize(self.prefix1+"onethousand"), 1000)
        with self.assertRaises(errors.ResourceNotFound):
            self.fs.getsize(self.prefix1+"doesnotexist")

    def test_getsyspath(self):
        self.fs.create(self.prefix1+"foo")
        try:
            syspath = self.fs.getsyspath(self.prefix1+"foo")
        except errors.NoSysPath:
            self.assertFalse(self.fs.hassyspath(self.prefix1+"foo"))
        else:
            self.assertIsInstance(syspath, text_type)
            self.assertIsInstance(self.fs.getospath(self.prefix1+"foo"), bytes)
            self.assertTrue(self.fs.hassyspath(self.prefix1+"foo"))
        # Should not throw an error
        self.fs.hassyspath(self.prefix1+"a/b/c/foo/bar")

    def test_geturl(self):
        self.fs.create(self.prefix1+"foo")
        try:
            self.fs.geturl(self.prefix1+"foo")
        except errors.NoURL:
            self.assertFalse(self.fs.hasurl(self.prefix1+"foo"))
        else:
            self.assertTrue(self.fs.hasurl(self.prefix1+"foo"))
        # Should not throw an error
        self.fs.hasurl(self.prefix1+"a/b/c/foo/bar")

    def test_geturl_purpose(self):
        """Check an unknown purpose raises a NoURL error.
        """
        self.fs.create(self.prefix1+"foo")
        with self.assertRaises(errors.NoURL):
            self.fs.geturl(self.prefix1+"foo", purpose="__nosuchpurpose__")

    def test_validatepath(self):
        """Check validatepath returns an absolute path.
        """
        path = self.fs.validatepath(self.prefix1+"foo")
        self.assertEqual(path, "/"+self.prefix1+"foo")

    def test_invalid_chars(self):
        # Test invalid path method.
        with self.assertRaises(errors.InvalidCharsInPath):
            self.fs.open(self.prefix1+"invalid\0file", "wb")

        with self.assertRaises(errors.InvalidCharsInPath):
            self.fs.validatepath(self.prefix1+"invalid\0file")

    def test_getinfo(self):
        # Test special case of root directory
        # Root directory has a name of ''
        root_info = self.fs.getinfo(self.prefix1+"/")
        self.assertEqual(root_info.name, "")
        self.assertTrue(root_info.is_dir)

        # Make a file of known size
        self.fs.writebytes(self.prefix1+"foo", b"bar")
        self.fs.makedir(self.prefix1+"dir")

        # Check basic namespace
        info = self.fs.getinfo(self.prefix1+"foo").raw
        self.assertIsInstance(info["basic"]["name"], text_type)
        self.assertEqual(info["basic"]["name"], "foo")
        self.assertFalse(info["basic"]["is_dir"])

        # Check basic namespace dir
        info = self.fs.getinfo(self.prefix1+"dir").raw
        self.assertEqual(info["basic"]["name"], "dir")
        self.assertTrue(info["basic"]["is_dir"])

        # Get the info
        info = self.fs.getinfo(self.prefix1+"foo", namespaces=["details"]).raw
        self.assertIsInstance(info, dict)
        self.assertEqual(info["details"]["size"], 3)
        self.assertEqual(info["details"]["type"], int(ResourceType.file))

        # Test getdetails
        self.assertEqual(info, self.fs.getdetails(self.prefix1+"foo").raw)

        # Raw info should be serializable
        try:
            json.dumps(info)
        except (TypeError, ValueError):
            raise AssertionError("info should be JSON serializable")

        # Non existant namespace is not an error
        no_info = self.fs.getinfo(self.prefix1+"foo", "__nosuchnamespace__").raw
        self.assertIsInstance(no_info, dict)
        self.assertEqual(no_info["basic"], {"name": "foo", "is_dir": False})

        # Check a number of standard namespaces
        # FS objects may not support all these, but we can at least
        # invoke the code
        info = self.fs.getinfo(self.prefix1+"foo", namespaces=["access", "stat", "details"])

        # Check that if the details namespace is present, times are
        # of valid types.
        if "details" in info.namespaces:
            details = info.raw["details"]
            self.assertIsInstance(details.get("accessed"), (type(None), int, float))
            self.assertIsInstance(details.get("modified"), (type(None), int, float))
            self.assertIsInstance(details.get("created"), (type(None), int, float))
            self.assertIsInstance(
                details.get("metadata_changed"), (type(None), int, float)
            )

    def test_exists(self):
        # Test exists method.
        # Check root directory always exists
        self.assertTrue(self.fs.exists(self.prefix1+"/"))
        self.assertTrue(self.fs.exists(self.prefix1+""))

        # Check files don't exist
        self.assertFalse(self.fs.exists(self.prefix1+"foo"))
        self.assertFalse(self.fs.exists(self.prefix1+"foo/bar"))
        self.assertFalse(self.fs.exists(self.prefix1+"foo/bar/baz"))
        self.assertFalse(self.fs.exists(self.prefix1+"egg"))

        # make some files and directories
        self.fs.makedirs(self.prefix1+"foo/bar")
        self.fs.writebytes(self.prefix1+"foo/bar/baz", b"test")

        # Check files exists
        self.assertTrue(self.fs.exists(self.prefix1+"foo"))
        self.assertTrue(self.fs.exists(self.prefix1+"foo/bar"))
        self.assertTrue(self.fs.exists(self.prefix1+"foo/bar/baz"))
        self.assertFalse(self.fs.exists(self.prefix1+"egg"))

        self.assert_exists("foo")
        self.assert_exists("foo/bar")
        self.assert_exists("foo/bar/baz")
        self.assert_not_exists("egg")

        # Delete a file
        self.fs.remove(self.prefix1+"foo/bar/baz")
        # Check it no longer exists
        self.assert_not_exists("foo/bar/baz")
        self.assertFalse(self.fs.exists(self.prefix1+"foo/bar/baz"))
        self.assert_not_exists("foo/bar/baz")

        # Check root directory always exists
        self.assertTrue(self.fs.exists(self.prefix1+"/"))
        self.assertTrue(self.fs.exists(self.prefix1+""))

    def test_listdir(self):
        # Check listing directory that doesn't exist
        with self.assertRaises(errors.ResourceNotFound):
            self.fs.listdir(self.prefix1+"foobar")

        # Check aliases for root
        self.assertEqual(self.fs.listdir(self.prefix1+"/"), [])
        self.assertEqual(self.fs.listdir(self.prefix1+"."), [])
        self.assertEqual(self.fs.listdir(self.prefix1+"./"), [])

        # Make a few objects
        self.fs.writebytes(self.prefix1+"foo", b"egg")
        self.fs.writebytes(self.prefix1+"bar", b"egg")
        self.fs.makedir(self.prefix1+"baz")

        # This should not be listed
        self.fs.writebytes(self.prefix1+"baz/egg", b"egg")

        # Check list works
        six.assertCountEqual(self, self.fs.listdir(self.prefix1+"/"), ["foo", "bar", "baz"])
        six.assertCountEqual(self, self.fs.listdir(self.prefix1+"."), ["foo", "bar", "baz"])
        six.assertCountEqual(self, self.fs.listdir(self.prefix1+"./"), ["foo", "bar", "baz"])

        # Check paths are unicode strings
        for name in self.fs.listdir(self.prefix1+"/"):
            self.assertIsInstance(name, text_type)

        # Create a subdirectory
        self.fs.makedir(self.prefix1+"dir")

        # Should start empty
        self.assertEqual(self.fs.listdir(self.prefix1+"/dir"), [])

        # Write some files
        self.fs.writebytes(self.prefix1+"dir/foofoo", b"egg")
        self.fs.writebytes(self.prefix1+"dir/barbar", b"egg")

        # Check listing subdirectory
        six.assertCountEqual(self, self.fs.listdir(self.prefix1+"dir"), ["foofoo", "barbar"])
        # Make sure they are unicode stringd
        for name in self.fs.listdir(self.prefix1+"dir"):
            self.assertIsInstance(name, text_type)

        self.fs.create(self.prefix1+"notadir")
        with self.assertRaises(errors.DirectoryExpected):
            self.fs.listdir(self.prefix1+"notadir")

    def test_move(self):
        # Make a file
        self.fs.writebytes(self.prefix1+"foo", b"egg")
        self.assert_isfile("foo")

        # Move it
        self.fs.move(self.prefix1+"foo", self.prefix1+"bar")

        # Check it has gone from original location
        self.assert_not_exists("foo")

        # Check it exists in the new location, and contents match
        self.assert_exists("bar")
        self.assert_bytes("bar", b"egg")

        # Check moving to existing file fails
        self.fs.writebytes(self.prefix1+"foo2", b"eggegg")
        with self.assertRaises(errors.DestinationExists):
            self.fs.move(self.prefix1+"foo2", self.prefix1+"bar")

        # Check move with overwrite=True
        self.fs.move(self.prefix1+"foo2", self.prefix1+"bar", overwrite=True)
        self.assert_not_exists("foo2")

        # Check moving to a non-existant directory
        with self.assertRaises(errors.ResourceNotFound):
            self.fs.move(self.prefix1+"bar", self.prefix1+"egg/bar")

        # Check moving an unexisting source
        with self.assertRaises(errors.ResourceNotFound):
            self.fs.move(self.prefix1+"egg", self.prefix1+"spam")

        # Check moving between different directories
        self.fs.makedir(self.prefix1+"baz")
        self.fs.writebytes(self.prefix1+"baz/bazbaz", b"bazbaz")
        self.fs.makedir(self.prefix1+"baz2")
        self.fs.move(self.prefix1+"baz/bazbaz", self.prefix1+"baz2/bazbaz")
        self.assert_not_exists("baz/bazbaz")
        self.assert_bytes("baz2/bazbaz", b"bazbaz")

        # Check moving a directory raises an error
        self.assert_isdir("baz2")
        self.assert_not_exists("yolk")
        with self.assertRaises(errors.FileExpected):
            self.fs.move(self.prefix1+"baz2", self.prefix1+"yolk")

    def test_makedir(self):
        # Check edge case of root
        with self.assertRaises(errors.DirectoryExists):
            self.fs.makedir(self.prefix1+"/")

        # Making root is a null op with recreate
        slash_fs = self.fs.makedir(self.prefix1+"/", recreate=True)
        self.assertIsInstance(slash_fs, SubFS)
        self.assertEqual(self.fs.listdir(self.prefix1+"/"), [])

        self.assert_not_exists("foo")
        self.fs.makedir(self.prefix1+"foo")
        self.assert_isdir("foo")
        self.assertEqual(self.fs.gettype(self.prefix1+"foo"), ResourceType.directory)
        self.fs.writebytes(self.prefix1+"foo/bar.txt", b"egg")
        self.assert_bytes("foo/bar.txt", b"egg")

        # Directory exists
        with self.assertRaises(errors.DirectoryExists):
            self.fs.makedir(self.prefix1+"foo")

        # Parent directory doesn't exist
        with self.assertRaises(errors.ResourceNotFound):
            self.fs.makedir(self.prefix1+"/foo/bar/baz")

        self.fs.makedir(self.prefix1+"/foo/bar")
        self.fs.makedir(self.prefix1+"/foo/bar/baz")

        with self.assertRaises(errors.DirectoryExists):
            self.fs.makedir(self.prefix1+"foo/bar/baz")

        with self.assertRaises(errors.DirectoryExists):
            self.fs.makedir(self.prefix1+"foo/bar.txt")

    def test_makedirs(self):
        self.assertFalse(self.fs.exists(self.prefix1+"foo"))
        self.fs.makedirs(self.prefix1+"foo")
        self.assertEqual(self.fs.gettype(self.prefix1+"foo"), ResourceType.directory)

        self.fs.makedirs(self.prefix1+"foo/bar/baz")
        self.assertTrue(self.fs.isdir(self.prefix1+"foo/bar"))
        self.assertTrue(self.fs.isdir(self.prefix1+"foo/bar/baz"))

        with self.assertRaises(errors.DirectoryExists):
            self.fs.makedirs(self.prefix1+"foo/bar/baz")

        self.fs.makedirs(self.prefix1+"foo/bar/baz", recreate=True)

        self.fs.writebytes(self.prefix1+"foo.bin", b"test")
        with self.assertRaises(errors.DirectoryExpected):
            self.fs.makedirs(self.prefix1+"foo.bin/bar")

        with self.assertRaises(errors.DirectoryExpected):
            self.fs.makedirs(self.prefix1+"foo.bin/bar/baz/egg")

    def test_repeat_dir(self):
        # Catches bug with directories contain repeated names,
        # discovered in s3fs
        self.fs.makedirs(self.prefix1+"foo/foo/foo")
        self.assertEqual(self.fs.listdir(self.prefix1+""), ["foo"])
        self.assertEqual(self.fs.listdir(self.prefix1+"foo"), ["foo"])
        self.assertEqual(self.fs.listdir(self.prefix1+"foo/foo"), ["foo"])
        self.assertEqual(self.fs.listdir(self.prefix1+"foo/foo/foo"), [])
        scan = list(self.fs.scandir(self.prefix1+"foo"))
        self.assertEqual(len(scan), 1)
        self.assertEqual(scan[0].name, "foo")

    def test_open(self):
        # Open a file that doesn't exist
        with self.assertRaises(errors.ResourceNotFound):
            self.fs.open(self.prefix1+"doesnotexist", "r")

        self.fs.makedir(self.prefix1+"foo")

        # Create a new text file
        text = "Hello, World"

        with self.fs.open(self.prefix1+"foo/hello", "wt") as f:
            repr(f)
            self.assertIsInstance(f, io.IOBase)
            self.assertTrue(f.writable())
            self.assertFalse(f.readable())
            self.assertFalse(f.closed)
            f.write(text)
        self.assertTrue(f.closed)

        # Read it back
        with self.fs.open(self.prefix1+"foo/hello", "rt") as f:
            self.assertIsInstance(f, io.IOBase)
            self.assertTrue(f.readable())
            self.assertFalse(f.writable())
            self.assertFalse(f.closed)
            hello = f.read()
        self.assertTrue(f.closed)
        self.assertEqual(hello, text)
        self.assert_text("foo/hello", text)

        # Test overwrite
        text = "Goodbye, World"
        with self.fs.open(self.prefix1+"foo/hello", "wt") as f:
            f.write(text)
        self.assert_text("foo/hello", text)

        # Open from missing dir
        with self.assertRaises(errors.ResourceNotFound):
            self.fs.open(self.prefix1+"/foo/bar/test.txt")

        # Test fileno returns a file number, if supported by the file.
        with self.fs.open(self.prefix1+"foo/hello") as f:
            try:
                fn = f.fileno()
            except io.UnsupportedOperation:
                pass
            else:
                self.assertEqual(os.read(fn, 7), b"Goodbye")

        # Test text files are proper iterators over themselves
        lines = os.linesep.join(["Line 1", "Line 2", "Line 3"])
        self.fs.writetext(self.prefix1+"iter.txt", lines)
        with self.fs.open(self.prefix1+"iter.txt") as f:
            for actual, expected in zip(f, lines.splitlines(1)):
                self.assertEqual(actual, expected)

    def test_openbin_rw(self):
        # Open a file that doesn't exist
        with self.assertRaises(errors.ResourceNotFound):
            self.fs.openbin(self.prefix1+"doesnotexist", "r")

        self.fs.makedir(self.prefix1+"foo")

        # Create a new text file
        text = b"Hello, World\n"

        with self.fs.openbin(self.prefix1+"foo/hello", "w") as f:
            repr(f)
            self.assertIsInstance(f, io.IOBase)
            self.assertTrue(f.writable())
            self.assertFalse(f.readable())
            self.assertEqual(len(text), f.write(text))
            self.assertFalse(f.closed)
        self.assertTrue(f.closed)

        with self.assertRaises(errors.FileExists):
            with self.fs.openbin(self.prefix1+"foo/hello", "x") as f:
                pass

        # Read it back
        with self.fs.openbin(self.prefix1+"foo/hello", "r") as f:
            self.assertIsInstance(f, io.IOBase)
            self.assertTrue(f.readable())
            self.assertFalse(f.writable())
            hello = f.read()
            self.assertFalse(f.closed)
        self.assertTrue(f.closed)
        self.assertEqual(hello, text)
        self.assert_bytes("foo/hello", text)

        # Test overwrite
        text = b"Goodbye, World"
        with self.fs.openbin(self.prefix1+"foo/hello", "w") as f:
            self.assertEqual(len(text), f.write(text))
        self.assert_bytes("foo/hello", text)

        # Test FileExpected raised
        with self.assertRaises(errors.FileExpected):
            self.fs.openbin(self.prefix1+"foo")  # directory

        # Open from missing dir
        with self.assertRaises(errors.ResourceNotFound):
            self.fs.openbin(self.prefix1+"/foo/bar/test.txt")

        # Test fileno returns a file number, if supported by the file.
        with self.fs.openbin(self.prefix1+"foo/hello") as f:
            try:
                fn = f.fileno()
            except io.UnsupportedOperation:
                pass
            else:
                self.assertEqual(os.read(fn, 7), b"Goodbye")

        # Test binary files are proper iterators over themselves
        lines = b"\n".join([b"Line 1", b"Line 2", b"Line 3"])
        self.fs.writebytes(self.prefix1+"iter.bin", lines)
        with self.fs.openbin(self.prefix1+"iter.bin") as f:
            for actual, expected in zip(f, lines.splitlines(1)):
                self.assertEqual(actual, expected)

    def test_open_files(self):
        # Test file-like objects work as expected.

        with self.fs.open(self.prefix1+"text", "w") as f:
            repr(f)
            text_type(f)
            self.assertIsInstance(f, io.IOBase)
            self.assertTrue(f.writable())
            self.assertFalse(f.readable())
            self.assertFalse(f.closed)
            self.assertEqual(f.tell(), 0)
            f.write("Hello\nWorld\n")
            self.assertEqual(f.tell(), 12)
            f.writelines(["foo\n", "bar\n", "baz\n"])
            with self.assertRaises(IOError):
                f.read(1)
        self.assertTrue(f.closed)

        with self.fs.open(self.prefix1+"bin", "wb") as f:
            with self.assertRaises(IOError):
                f.read(1)

        with self.fs.open(self.prefix1+"text", "r") as f:
            repr(f)
            text_type(f)
            self.assertIsInstance(f, io.IOBase)
            self.assertFalse(f.writable())
            self.assertTrue(f.readable())
            self.assertFalse(f.closed)
            self.assertEqual(
                f.readlines(), ["Hello\n", "World\n", "foo\n", "bar\n", "baz\n"]
            )
            with self.assertRaises(IOError):
                f.write("no")
        self.assertTrue(f.closed)

        with self.fs.open(self.prefix1+"text", "rb") as f:
            self.assertIsInstance(f, io.IOBase)
            self.assertFalse(f.writable())
            self.assertTrue(f.readable())
            self.assertFalse(f.closed)
            self.assertEqual(f.readlines(8), [b"Hello\n", b"World\n"])
            self.assertEqual(f.tell(), 12)
            buffer = bytearray(4)
            self.assertEqual(f.readinto(buffer), 4)
            self.assertEqual(f.tell(), 16)
            self.assertEqual(buffer, b"foo\n")
            with self.assertRaises(IOError):
                f.write(b"no")
        self.assertTrue(f.closed)

        with self.fs.open(self.prefix1+"text", "r") as f:
            self.assertEqual(list(f), ["Hello\n", "World\n", "foo\n", "bar\n", "baz\n"])
            self.assertFalse(f.closed)
        self.assertTrue(f.closed)

        iter_lines = iter(self.fs.open(self.prefix1+"text"))
        self.assertEqual(next(iter_lines), "Hello\n")

        with self.fs.open(self.prefix1+"unicode", "w") as f:
            self.assertEqual(12, f.write("Héllo\nWörld\n"))

        with self.fs.open(self.prefix1+"text", "rb") as f:
            self.assertIsInstance(f, io.IOBase)
            self.assertFalse(f.writable())
            self.assertTrue(f.readable())
            self.assertTrue(f.seekable())
            self.assertFalse(f.closed)
            self.assertEqual(f.read(1), b"H")
            self.assertEqual(3, f.seek(3, Seek.set))
            self.assertEqual(f.read(1), b"l")
            self.assertEqual(6, f.seek(2, Seek.current))
            self.assertEqual(f.read(1), b"W")
            self.assertEqual(22, f.seek(-2, Seek.end))
            self.assertEqual(f.read(1), b"z")
            with self.assertRaises(ValueError):
                f.seek(10, 77)
        self.assertTrue(f.closed)

        with self.fs.open(self.prefix1+"text", "r+b") as f:
            self.assertIsInstance(f, io.IOBase)
            self.assertTrue(f.readable())
            self.assertTrue(f.writable())
            self.assertTrue(f.seekable())
            self.assertFalse(f.closed)
            self.assertEqual(5, f.seek(5))
            self.assertEqual(5, f.truncate())
            self.assertEqual(0, f.seek(0))
            self.assertEqual(f.read(), b"Hello")
            self.assertEqual(10, f.truncate(10))
            self.assertEqual(5, f.tell())
            self.assertEqual(0, f.seek(0))
            self.assertEqual(f.read(), b"Hello\0\0\0\0\0")
            self.assertEqual(4, f.seek(4))
            f.write(b"O")
            self.assertEqual(4, f.seek(4))
            self.assertEqual(f.read(1), b"O")
        self.assertTrue(f.closed)

    def test_openbin(self):
        # Write a binary file
        with self.fs.openbin(self.prefix1+"file.bin", "wb") as write_file:
            repr(write_file)
            text_type(write_file)
            self.assertIsInstance(write_file, io.IOBase)
            self.assertTrue(write_file.writable())
            self.assertFalse(write_file.readable())
            self.assertFalse(write_file.closed)
            self.assertEqual(3, write_file.write(b"\0\1\2"))
        self.assertTrue(write_file.closed)

        # Read a binary file
        with self.fs.openbin(self.prefix1+"file.bin", "rb") as read_file:
            repr(write_file)
            text_type(write_file)
            self.assertIsInstance(read_file, io.IOBase)
            self.assertTrue(read_file.readable())
            self.assertFalse(read_file.writable())
            self.assertFalse(read_file.closed)
            data = read_file.read()
        self.assertEqual(data, b"\0\1\2")
        self.assertTrue(read_file.closed)

        # Check disallow text mode
        with self.assertRaises(ValueError):
            with self.fs.openbin(self.prefix1+"file.bin", "rt") as read_file:
                pass

        # Check errors
        with self.assertRaises(errors.ResourceNotFound):
            self.fs.openbin(self.prefix1+"foo.bin")

        # Open from missing dir
        with self.assertRaises(errors.ResourceNotFound):
            self.fs.openbin(self.prefix1+"/foo/bar/test.txt")

        self.fs.makedir(self.prefix1+"foo")
        # Attempt to open a directory
        with self.assertRaises(errors.FileExpected):
            self.fs.openbin(self.prefix1+"/foo")

        # Attempt to write to a directory
        with self.assertRaises(errors.FileExpected):
            self.fs.openbin(self.prefix1+"/foo", "w")

        # Opening a file in a directory which doesn't exist
        with self.assertRaises(errors.ResourceNotFound):
            self.fs.openbin(self.prefix1+"/egg/bar")

        # Opening a file in a directory which doesn't exist
        with self.assertRaises(errors.ResourceNotFound):
            self.fs.openbin(self.prefix1+"/egg/bar", "w")

        # Opening with a invalid mode
        with self.assertRaises(ValueError):
            self.fs.openbin(self.prefix1+"foo.bin", "h")

    def test_open_exclusive(self):
        with self.fs.open(self.prefix1+"test_open_exclusive", "x") as f:
            f.write("bananas")

        with self.assertRaises(errors.FileExists):
            self.fs.open(self.prefix1+"test_open_exclusive", "x")

    def test_openbin_exclusive(self):
        with self.fs.openbin(self.prefix1+"test_openbin_exclusive", "x") as f:
            f.write(b"bananas")

        with self.assertRaises(errors.FileExists):
            self.fs.openbin(self.prefix1+"test_openbin_exclusive", "x")

    def test_opendir(self):
        # Make a simple directory structure
        self.fs.makedir(self.prefix1+"foo")
        self.fs.writebytes(self.prefix1+"foo/bar", b"barbar")
        self.fs.writebytes(self.prefix1+"foo/egg", b"eggegg")

        # Open a sub directory
        with self.fs.opendir(self.prefix1+"foo") as foo_fs:
            repr(foo_fs)
            text_type(foo_fs)
            six.assertCountEqual(self, foo_fs.listdir("/"), ["bar", "egg"])
            self.assertTrue(foo_fs.isfile("bar"))
            self.assertTrue(foo_fs.isfile("egg"))
            self.assertEqual(foo_fs.readbytes("bar"), b"barbar")
            self.assertEqual(foo_fs.readbytes("egg"), b"eggegg")

        self.assertFalse(self.fs.isclosed())

        # Attempt to open a non-existent directory
        with self.assertRaises(errors.ResourceNotFound):
            self.fs.opendir(self.prefix1+"egg")

        # Check error when doing opendir on a non dir
        with self.assertRaises(errors.DirectoryExpected):
            self.fs.opendir(self.prefix1+"foo/egg")

        # These should work, and will essentially return a 'clone' of sorts
        self.fs.opendir(self.prefix1+"")
        self.fs.opendir(self.prefix1+"/")

        # Check ClosingSubFS closes 'parent'
        with self.fs.opendir(self.prefix1+"foo", factory=ClosingSubFS) as foo_fs:
            six.assertCountEqual(self, foo_fs.listdir("/"), ["bar", "egg"])
            self.assertTrue(foo_fs.isfile("bar"))
            self.assertTrue(foo_fs.isfile("egg"))
            self.assertEqual(foo_fs.readbytes("bar"), b"barbar")
            self.assertEqual(foo_fs.readbytes("egg"), b"eggegg")

        self.assertTrue(self.fs.isclosed())

    def test_remove(self):

        self.fs.writebytes(self.prefix1+"foo1", b"test1")
        self.fs.writebytes(self.prefix1+"foo2", b"test2")
        self.fs.writebytes(self.prefix1+"foo3", b"test3")

        self.assert_isfile("foo1")
        self.assert_isfile("foo2")
        self.assert_isfile("foo3")

        self.fs.remove(self.prefix1+"foo2")

        self.assert_isfile("foo1")
        self.assert_not_exists("foo2")
        self.assert_isfile("foo3")

        with self.assertRaises(errors.ResourceNotFound):
            self.fs.remove(self.prefix1+"bar")

        self.fs.makedir(self.prefix1+"dir")
        with self.assertRaises(errors.FileExpected):
            self.fs.remove(self.prefix1+"dir")

        self.fs.makedirs(self.prefix1+"foo/bar/baz/")

        error_msg = "resource 'foo/bar/egg/test.txt' not found"
        assertRaisesRegex = getattr(self, "assertRaisesRegex", self.assertRaisesRegexp)
        with assertRaisesRegex(errors.ResourceNotFound, error_msg):
            self.fs.remove(self.prefix1+"foo/bar/egg/test.txt")

    def test_removedir(self):

        # Test removing root
        with self.assertRaises(errors.RemoveRootError):
            self.fs.removedir(self.prefix1+"/")

        self.fs.makedirs(self.prefix1+"foo/bar/baz")
        self.assertTrue(self.fs.exists(self.prefix1+"foo/bar/baz"))
        self.fs.removedir(self.prefix1+"foo/bar/baz")
        self.assertFalse(self.fs.exists(self.prefix1+"foo/bar/baz"))
        self.assertTrue(self.fs.isdir(self.prefix1+"foo/bar"))

        with self.assertRaises(errors.ResourceNotFound):
            self.fs.removedir(self.prefix1+"nodir")

        # Test force removal
        self.fs.makedirs(self.prefix1+"foo/bar/baz")
        self.fs.writebytes(self.prefix1+"foo/egg", b"test")

        with self.assertRaises(errors.DirectoryExpected):
            self.fs.removedir(self.prefix1+"foo/egg")

        with self.assertRaises(errors.DirectoryNotEmpty):
            self.fs.removedir(self.prefix1+"foo/bar")

    def test_removetree(self):
        self.fs.makedirs(self.prefix1+"foo/bar/baz")
        self.fs.makedirs(self.prefix1+"foo/egg")
        self.fs.makedirs(self.prefix1+"foo/a/b/c/d/e")
        self.fs.create(self.prefix1+"foo/egg.txt")
        self.fs.create(self.prefix1+"foo/bar/egg.bin")
        self.fs.create(self.prefix1+"foo/bar/baz/egg.txt")
        self.fs.create(self.prefix1+"foo/a/b/c/1.txt")
        self.fs.create(self.prefix1+"foo/a/b/c/2.txt")
        self.fs.create(self.prefix1+"foo/a/b/c/3.txt")

        self.assert_exists("foo/egg.txt")
        self.assert_exists("foo/bar/egg.bin")

        self.fs.removetree(self.prefix1+"foo")
        self.assert_not_exists("foo")

    def test_setinfo(self):
        self.fs.create(self.prefix1+"birthday.txt")
        now = math.floor(time.time())

        change_info = {"details": {"accessed": now + 60, "modified": now + 60 * 60}}
        self.fs.setinfo(self.prefix1+"birthday.txt", change_info)
        new_info = self.fs.getinfo(self.prefix1+"birthday.txt", namespaces=["details"]).raw
        if "accessed" in new_info.get("_write", []):
            self.assertEqual(new_info["details"]["accessed"], now + 60)
        if "modified" in new_info.get("_write", []):
            self.assertEqual(new_info["details"]["modified"], now + 60 * 60)

        with self.assertRaises(errors.ResourceNotFound):
            self.fs.setinfo(self.prefix1+"nothing", {})

    def test_settimes(self):
        self.fs.create(self.prefix1+"birthday.txt")
        self.fs.settimes(self.prefix1+"birthday.txt", accessed=datetime(2016, 7, 5))
        info = self.fs.getinfo(self.prefix1+"birthday.txt", namespaces=["details"])
        writeable = info.get("details", "_write", [])
        if "accessed" in writeable:
            self.assertEqual(info.accessed, datetime(2016, 7, 5, tzinfo=pytz.UTC))
        if "modified" in writeable:
            self.assertEqual(info.modified, datetime(2016, 7, 5, tzinfo=pytz.UTC))

    def test_touch(self):
        self.fs.touch(self.prefix1+"new.txt")
        self.assert_isfile("new.txt")
        self.fs.settimes(self.prefix1+"new.txt", datetime(2016, 7, 5))
        info = self.fs.getinfo(self.prefix1+"new.txt", namespaces=["details"])
        if info.is_writeable("details", "accessed"):
            self.assertEqual(info.accessed, datetime(2016, 7, 5, tzinfo=pytz.UTC))
            now = time.time()
            self.fs.touch(self.prefix1+"new.txt")
            accessed = self.fs.getinfo(self.prefix1+"new.txt", namespaces=["details"]).raw[
                "details"
            ]["accessed"]
            self.assertTrue(accessed - now < 5)

    def test_close(self):
        self.assertFalse(self.fs.isclosed())
        self.fs.close()
        self.assertTrue(self.fs.isclosed())
        # Check second close call is a no-op
        self.fs.close()
        self.assertTrue(self.fs.isclosed())

        # Check further operations raise a FilesystemClosed exception
        with self.assertRaises(errors.FilesystemClosed):
            self.fs.openbin(self.prefix1+"test.bin")

    def test_copy(self):
        # Test copy to new path
        self.fs.writebytes(self.prefix1+"foo", b"test")
        self.fs.copy(self.prefix1+"foo", self.prefix1+"bar")
        self.assert_bytes("bar", b"test")

        # Test copy over existing path
        self.fs.writebytes(self.prefix1+"baz", b"truncateme")
        self.fs.copy(self.prefix1+"foo", self.prefix1+"baz", overwrite=True)
        self.assert_bytes("foo", b"test")

        # Test copying a file to a destination that exists
        with self.assertRaises(errors.DestinationExists):
            self.fs.copy(self.prefix1+"baz", self.prefix1+"foo")

        # Test copying to a directory that doesn't exist
        with self.assertRaises(errors.ResourceNotFound):
            self.fs.copy(self.prefix1+"baz", self.prefix1+"a/b/c/baz")

        # Test copying a source that doesn't exist
        with self.assertRaises(errors.ResourceNotFound):
            self.fs.copy(self.prefix1+"egg", self.prefix1+"spam")

        # Test copying a directory
        self.fs.makedir(self.prefix1+"dir")
        with self.assertRaises(errors.FileExpected):
            self.fs.copy(self.prefix1+"dir", self.prefix1+"folder")

    def _test_upload(self, workers):
        """Test fs.copy with varying number of worker threads."""
        data1 = b"foo" * 256 * 1024
        data2 = b"bar" * 2 * 256 * 1024
        data3 = b"baz" * 3 * 256 * 1024
        data4 = b"egg" * 7 * 256 * 1024

        with open_fs("temp://") as src_fs:
            if self.prefix1 != "":
                src_fs.makedir(self.prefix1)
            src_fs.writebytes(self.prefix1+"foo", data1)
            src_fs.writebytes(self.prefix1+"bar", data2)
            src_fs.makedir(self.prefix1+"dir1").writebytes("baz", data3)
            src_fs.makedirs(self.prefix1+"dir2/dir3").writebytes("egg", data4)
            dst_fs = self.fs
            fs.copy.copy_fs(src_fs, dst_fs, workers=workers)
            self.assertEqual(dst_fs.readbytes(self.prefix1+"foo"), data1)
            self.assertEqual(dst_fs.readbytes(self.prefix1+"bar"), data2)
            self.assertEqual(dst_fs.readbytes(self.prefix1+"dir1/baz"), data3)
            self.assertEqual(dst_fs.readbytes(self.prefix1+"dir2/dir3/egg"), data4)

    def test_upload_0(self):
        self._test_upload(0)

    def test_upload_1(self):
        self._test_upload(1)

    def test_upload_2(self):
        self._test_upload(2)

    def test_upload_4(self):
        self._test_upload(4)

    def _test_download(self, workers):
        """Test fs.copy with varying number of worker threads."""
        data1 = b"foo" * 256 * 1024
        data2 = b"bar" * 2 * 256 * 1024
        data3 = b"baz" * 3 * 256 * 1024
        data4 = b"egg" * 7 * 256 * 1024
        src_fs = self.fs
        with open_fs("temp://") as dst_fs:
            src_fs.writebytes(self.prefix1+"foo", data1)
            src_fs.writebytes(self.prefix1+"bar", data2)
            src_fs.makedir(self.prefix1+"dir1").writebytes("baz", data3)
            src_fs.makedirs(self.prefix1+"dir2/dir3").writebytes("egg", data4)
            fs.copy.copy_fs("s3://{0}".format(self.prefix1), dst_fs, workers=workers)
            self.assertEqual(dst_fs.readbytes("foo"), data1)
            self.assertEqual(dst_fs.readbytes("bar"), data2)
            self.assertEqual(dst_fs.readbytes("dir1/baz"), data3)
            self.assertEqual(dst_fs.readbytes("dir2/dir3/egg"), data4)

    def test_download_0(self):
        self._test_download(0)

    def test_download_1(self):
        self._test_download(1)

    def test_download_2(self):
        self._test_download(2)

    def test_download_4(self):
        self._test_download(4)

    def test_create(self):
        # Test create new file
        self.assertFalse(self.fs.exists(self.prefix1+"foo"))
        self.fs.create(self.prefix1+"foo")
        self.assertTrue(self.fs.exists(self.prefix1+"foo"))
        self.assertEqual(self.fs.gettype(self.prefix1+"foo"), ResourceType.file)
        self.assertEqual(self.fs.getsize(self.prefix1+"foo"), 0)

        # Test wipe existing file
        self.fs.writebytes(self.prefix1+"foo", b"bar")
        self.assertEqual(self.fs.getsize(self.prefix1+"foo"), 3)
        self.fs.create(self.prefix1+"foo", wipe=True)
        self.assertEqual(self.fs.getsize(self.prefix1+"foo"), 0)

        # Test create with existing file, and not wipe
        self.fs.writebytes(self.prefix1+"foo", b"bar")
        self.assertEqual(self.fs.getsize(self.prefix1+"foo"), 3)
        self.fs.create(self.prefix1+"foo", wipe=False)
        self.assertEqual(self.fs.getsize(self.prefix1+"foo"), 3)

    def test_desc(self):
        # Describe a file
        self.fs.create(self.prefix1+"foo")
        description = self.fs.desc(self.prefix1+"foo")
        self.assertIsInstance(description, text_type)

        # Describe a dir
        self.fs.makedir(self.prefix1+"dir")
        self.fs.desc(self.prefix1+"dir")

        # Special cases that may hide bugs
        self.fs.desc(self.prefix1+"/")
        self.fs.desc(self.prefix1+"")

        with self.assertRaises(errors.ResourceNotFound):
            self.fs.desc(self.prefix1+"bar")

    def test_scandir(self):
        # Check exception for scanning dir that doesn't exist
        with self.assertRaises(errors.ResourceNotFound):
            for _info in self.fs.scandir(self.prefix1+"/foobar"):
                pass

        # Check scandir returns an iterable
        iter_scandir = self.fs.scandir(self.prefix1+"/")
        self.assertTrue(isinstance(iter_scandir, collections_abc.Iterable))
        self.assertEqual(list(iter_scandir), [])

        # Check scanning
        self.fs.create(self.prefix1+"foo")

        # Can't scandir on a file
        with self.assertRaises(errors.DirectoryExpected):
            list(self.fs.scandir(self.prefix1+"foo"))

        self.fs.create(self.prefix1+"bar")
        self.fs.makedir(self.prefix1+"dir")
        iter_scandir = self.fs.scandir(self.prefix1+"/")
        self.assertTrue(isinstance(iter_scandir, collections_abc.Iterable))

        scandir = sorted(
            (r.raw for r in iter_scandir), key=lambda info: info["basic"]["name"]
        )

        # Filesystems may send us more than we ask for
        # We just want to test the 'basic' namespace
        scandir = [{"basic": i["basic"]} for i in scandir]

        self.assertEqual(
            scandir,
            [
                {"basic": {"name": "bar", "is_dir": False}},
                {"basic": {"name": "dir", "is_dir": True}},
                {"basic": {"name": "foo", "is_dir": False}},
            ],
        )

        # Hard to test optional namespaces, but at least run the code
        list(
            self.fs.scandir(self.prefix1+
                "/", namespaces=["details", "link", "stat", "lstat", "access"]
            )
        )

        # Test paging
        page1 = list(self.fs.scandir(self.prefix1+"/", page=(None, 2)))
        self.assertEqual(len(page1), 2)
        page2 = list(self.fs.scandir(self.prefix1+"/", page=(2, 4)))
        self.assertEqual(len(page2), 1)
        page3 = list(self.fs.scandir(self.prefix1+"/", page=(4, 6)))
        self.assertEqual(len(page3), 0)
        paged = {r.name for r in itertools.chain(page1, page2)}
        self.assertEqual(paged, {"foo", "bar", "dir"})

    def test_filterdir(self):
        self.assertEqual(list(self.fs.filterdir(self.prefix1+"/", files=["*.py"])), [])

        self.fs.makedir(self.prefix1+"bar")
        self.fs.create(self.prefix1+"foo.txt")
        self.fs.create(self.prefix1+"foo.py")
        self.fs.create(self.prefix1+"foo.pyc")

        page1 = list(self.fs.filterdir(self.prefix1+"/", page=(None, 2)))
        page2 = list(self.fs.filterdir(self.prefix1+"/", page=(2, 4)))
        page3 = list(self.fs.filterdir(self.prefix1+"/", page=(4, 6)))

        self.assertEqual(len(page1), 2)
        self.assertEqual(len(page2), 2)
        self.assertEqual(len(page3), 0)
        names = [info.name for info in itertools.chain(page1, page2, page3)]
        self.assertEqual(set(names), {"foo.txt", "foo.py", "foo.pyc", "bar"})

        # Check filtering by wildcard
        dir_list = [info.name for info in self.fs.filterdir(self.prefix1+"/", files=["*.py"])]
        self.assertEqual(set(dir_list), {"bar", "foo.py"})

        # Check filtering by miltiple wildcard
        dir_list = [
            info.name for info in self.fs.filterdir(self.prefix1+"/", files=["*.py", "*.pyc"])
        ]
        self.assertEqual(set(dir_list), {"bar", "foo.py", "foo.pyc"})

        # Check excluding dirs
        dir_list = [
            info.name
            for info in self.fs.filterdir(self.prefix1+
                "/", exclude_dirs=["*"], files=["*.py", "*.pyc"]
            )
        ]
        self.assertEqual(set(dir_list), {"foo.py", "foo.pyc"})

        # Check excluding files
        dir_list = [info.name for info in self.fs.filterdir(self.prefix1+"/", exclude_files=["*"])]
        self.assertEqual(set(dir_list), {"bar"})

        # Check wildcards must be a list
        with self.assertRaises(TypeError):
            dir_list = [info.name for info in self.fs.filterdir(self.prefix1+"/", files="*.py")]

        self.fs.makedir(self.prefix1+"baz")
        dir_list = [
            info.name
            for info in self.fs.filterdir(self.prefix1+"/", exclude_files=["*"], dirs=["??z"])
        ]
        self.assertEqual(set(dir_list), {"baz"})

        with self.assertRaises(TypeError):
            dir_list = [
                info.name
                for info in self.fs.filterdir(self.prefix1+"/", exclude_files=["*"], dirs="*.py")
            ]

    def test_readbytes(self):
        # Test readbytes method.
        all_bytes = b"".join(six.int2byte(n) for n in range(256))
        with self.fs.open(self.prefix1+"foo", "wb") as f:
            f.write(all_bytes)
        self.assertEqual(self.fs.readbytes(self.prefix1+"foo"), all_bytes)
        _all_bytes = self.fs.readbytes(self.prefix1+"foo")
        self.assertIsInstance(_all_bytes, bytes)
        self.assertEqual(_all_bytes, all_bytes)

        with self.assertRaises(errors.ResourceNotFound):
            self.fs.readbytes(self.prefix1+"foo/bar")

        self.fs.makedir(self.prefix1+"baz")
        with self.assertRaises(errors.FileExpected):
            self.fs.readbytes(self.prefix1+"baz")

    def test_download(self):
        test_bytes = b"Hello, World"
        self.fs.writebytes(self.prefix1+"hello.bin", test_bytes)
        write_file = io.BytesIO()
        self.fs.download(self.prefix1+"hello.bin", write_file)
        self.assertEqual(write_file.getvalue(), test_bytes)

        with self.assertRaises(errors.ResourceNotFound):
            self.fs.download(self.prefix1+"foo.bin", write_file)

    def test_download_chunk_size(self):
        test_bytes = b"Hello, World" * 100
        self.fs.writebytes(self.prefix1+"hello.bin", test_bytes)
        write_file = io.BytesIO()
        self.fs.download(self.prefix1+"hello.bin", write_file, chunk_size=8)
        self.assertEqual(write_file.getvalue(), test_bytes)

    def test_isempty(self):
        self.assertTrue(self.fs.isempty(self.prefix1+"/"))
        self.fs.makedir(self.prefix1+"foo")
        self.assertFalse(self.fs.isempty(self.prefix1+"/"))
        self.assertTrue(self.fs.isempty(self.prefix1+"/foo"))
        self.fs.create(self.prefix1+"foo/bar.txt")
        self.assertFalse(self.fs.isempty(self.prefix1+"/foo"))
        self.fs.remove(self.prefix1+"foo/bar.txt")
        self.assertTrue(self.fs.isempty(self.prefix1+"/foo"))

    def test_writebytes(self):
        all_bytes = b"".join(six.int2byte(n) for n in range(256))
        self.fs.writebytes(self.prefix1+"foo", all_bytes)
        with self.fs.open(self.prefix1+"foo", "rb") as f:
            _bytes = f.read()
        self.assertIsInstance(_bytes, bytes)
        self.assertEqual(_bytes, all_bytes)
        self.assert_bytes("foo", all_bytes)
        with self.assertRaises(TypeError):
            self.fs.writebytes(self.prefix1+"notbytes", "unicode")

    def test_readtext(self):
        self.fs.makedir(self.prefix1+"foo")
        with self.fs.open(self.prefix1+"foo/unicode.txt", "wt") as f:
            f.write(UNICODE_TEXT)
        text = self.fs.readtext(self.prefix1+"foo/unicode.txt")
        self.assertIsInstance(text, text_type)
        self.assertEqual(text, UNICODE_TEXT)
        self.assert_text("foo/unicode.txt", UNICODE_TEXT)

    def test_writetext(self):
        # Test writetext method.
        self.fs.writetext(self.prefix1+"foo", "bar")
        with self.fs.open(self.prefix1+"foo", "rt") as f:
            foo = f.read()
        self.assertEqual(foo, "bar")
        self.assertIsInstance(foo, text_type)
        with self.assertRaises(TypeError):
            self.fs.writetext(self.prefix1+"nottext", b"bytes")

    def test_writefile(self):
        bytes_file = io.BytesIO(b"bar")
        self.fs.writefile(self.prefix1+"foo", bytes_file)
        with self.fs.open(self.prefix1+"foo", "rb") as f:
            data = f.read()
        self.assertEqual(data, b"bar")

    def test_upload(self):
        bytes_file = io.BytesIO(b"bar")
        self.fs.upload(self.prefix1+"foo", bytes_file)
        with self.fs.open(self.prefix1+"foo", "rb") as f:
            data = f.read()
        self.assertEqual(data, b"bar")

    def test_upload_chunk_size(self):
        test_data = b"bar" * 128
        bytes_file = io.BytesIO(test_data)
        self.fs.upload(self.prefix1+"foo", bytes_file, chunk_size=8)
        with self.fs.open(self.prefix1+"foo", "rb") as f:
            data = f.read()
        self.assertEqual(data, test_data)

    def test_bin_files(self):
        # Check binary files.
        with self.fs.openbin(self.prefix1+"foo1", "wb") as f:
            text_type(f)
            repr(f)
            f.write(b"a")
            f.write(b"b")
            f.write(b"c")
        self.assert_bytes("foo1", b"abc")

        # Test writelines
        with self.fs.openbin(self.prefix1+"foo2", "wb") as f:
            f.writelines([b"hello\n", b"world"])
        self.assert_bytes("foo2", b"hello\nworld")

        # Test readline
        with self.fs.openbin(self.prefix1+"foo2") as f:
            self.assertEqual(f.readline(), b"hello\n")
            self.assertEqual(f.readline(), b"world")

        # Test readlines
        with self.fs.openbin(self.prefix1+"foo2") as f:
            lines = f.readlines()
        self.assertEqual(lines, [b"hello\n", b"world"])
        with self.fs.openbin(self.prefix1+"foo2") as f:
            lines = list(f)
        self.assertEqual(lines, [b"hello\n", b"world"])
        with self.fs.openbin(self.prefix1+"foo2") as f:
            lines = []
            for line in f:
                lines.append(line)
        self.assertEqual(lines, [b"hello\n", b"world"])
        with self.fs.openbin(self.prefix1+"foo2") as f:
            self.assertEqual(next(f), b"hello\n")

        # Test truncate
        with self.fs.open(self.prefix1+"foo2", "r+b") as f:
            f.truncate(3)
        self.assertEqual(self.fs.getsize(self.prefix1+"foo2"), 3)
        self.assert_bytes("foo2", b"hel")

    def test_files(self):
        # Test multiple writes

        with self.fs.open(self.prefix1+"foo1", "wt") as f:
            text_type(f)
            repr(f)
            f.write("a")
            f.write("b")
            f.write("c")
        self.assert_text("foo1", "abc")

        # Test writelines
        with self.fs.open(self.prefix1+"foo2", "wt") as f:
            f.writelines(["hello\n", "world"])
        self.assert_text("foo2", "hello\nworld")

        # Test readline
        with self.fs.open(self.prefix1+"foo2") as f:
            self.assertEqual(f.readline(), "hello\n")
            self.assertEqual(f.readline(), "world")

        # Test readlines
        with self.fs.open(self.prefix1+"foo2") as f:
            lines = f.readlines()
        self.assertEqual(lines, ["hello\n", "world"])
        with self.fs.open(self.prefix1+"foo2") as f:
            lines = list(f)
        self.assertEqual(lines, ["hello\n", "world"])
        with self.fs.open(self.prefix1+"foo2") as f:
            lines = []
            for line in f:
                lines.append(line)
        self.assertEqual(lines, ["hello\n", "world"])

        # Test truncate
        with self.fs.open(self.prefix1+"foo2", "r+") as f:
            f.truncate(3)
        self.assertEqual(self.fs.getsize(self.prefix1+"foo2"), 3)
        self.assert_text("foo2", "hel")

        with self.fs.open(self.prefix1+"foo2", "ab") as f:
            f.write(b"p")
        self.assert_bytes("foo2", b"help")

        # Test __del__ doesn't throw traceback
        f = self.fs.open(self.prefix1+"foo2", "r")
        del f

        with self.assertRaises(IOError):
            with self.fs.open(self.prefix1+"foo2", "r") as f:
                f.write("no!")

        with self.assertRaises(IOError):
            with self.fs.open(self.prefix1+"newfoo", "w") as f:
                f.read(2)

    def test_copy_file(self):
        # Test fs.copy.copy_file
        bytes_test = b"Hello, World"
        self.fs.writebytes(self.prefix1+"foo.txt", bytes_test)
        fs.copy.copy_file(self.fs, self.prefix1+"foo.txt", self.fs, self.prefix1+"bar.txt")
        self.assert_bytes("bar.txt", bytes_test)

        mem_fs = open_fs("mem://")

        fs.copy.copy_file(self.fs, self.prefix1+"foo.txt", mem_fs, "bar.txt")
        self.assertEqual(mem_fs.readbytes("bar.txt"), bytes_test)

    def test_copy_structure(self):
        mem_fs = open_fs("mem://")
        self.fs.makedirs(self.prefix1+"foo/bar/baz")
        self.fs.makedir(self.prefix1+"egg")

        fs.copy.copy_structure('s3://{0}'.format(self.prefix1), mem_fs)
        expected = {"/egg", "/foo", "/foo/bar", "/foo/bar/baz"}
        self.assertEqual(set(walk.walk_dirs(mem_fs)), expected)

    def _test_copy_dir(self, protocol):
        # Test copy.copy_dir.

        # Test copying to a another fs
        other_fs = open_fs(protocol)
        self.fs.makedirs(self.prefix1+"foo/bar/baz")
        self.fs.makedir(self.prefix1+"egg")
        self.fs.writetext(self.prefix1+"top.txt", "Hello, World")
        self.fs.writetext(self.prefix1+"/foo/bar/baz/test.txt", "Goodbye, World")

        fs.copy.copy_dir(self.fs, self.prefix1+"/", other_fs, "/")
        expected = {"/egg", "/foo", "/foo/bar", "/foo/bar/baz"}
        self.assertEqual(set(walk.walk_dirs(other_fs)), expected)
        self.assert_text("top.txt", "Hello, World")
        self.assert_text("/foo/bar/baz/test.txt", "Goodbye, World")

        # Test copying a sub dir
        other_fs = open_fs("mem://")
        fs.copy.copy_dir(self.fs, self.prefix1+"/foo", other_fs, "/")
        self.assertEqual(list(walk.walk_files(other_fs)), ["/bar/baz/test.txt"])

        print("BEFORE")
        self.fs.tree(path=self.prefix1)
        other_fs.tree()
        fs.copy.copy_dir(self.fs, self.prefix1+"/foo", other_fs, "/egg")

        print("FS")
        self.fs.tree(path=self.prefix1)
        print("OTHER")
        other_fs.tree()
        self.assertEqual(
            list(walk.walk_files(other_fs)),
            ["/bar/baz/test.txt", "/egg/bar/baz/test.txt"],
        )

    def _test_copy_dir_write(self, protocol):
        # Test copying to this filesystem from another.

        other_fs = open_fs(protocol)
        other_fs.makedirs("foo/bar/baz")
        other_fs.makedir("egg")
        other_fs.writetext("top.txt", "Hello, World")
        other_fs.writetext("/foo/bar/baz/test.txt", "Goodbye, World")
        fs.copy.copy_dir(other_fs, "/", self.fs, self.prefix1+"/")
        expected = {self._prefix1+"/egg", self._prefix1+"/foo", self._prefix1+"/foo/bar", self._prefix1+"/foo/bar/baz"}
        self.assertEqual(set(walk.walk_dirs(self.fs, path=self.prefix1)), expected)
        self.assert_text("top.txt", "Hello, World")
        self.assert_text("/foo/bar/baz/test.txt", "Goodbye, World")

    def test_copy_dir_mem(self):
        # Test copy_dir with a mem fs.
        self._test_copy_dir("mem://")
        self._test_copy_dir_write("mem://")

    def test_copy_dir_temp(self):
        # Test copy_dir with a temp fs.
        self._test_copy_dir("temp://")
        self._test_copy_dir_write("temp://")

    def _test_move_dir_write(self, protocol):
        # Test moving to this filesystem from another.
        other_fs = open_fs(protocol)
        other_fs.makedirs("foo/bar/baz")
        other_fs.makedir("egg")
        other_fs.writetext("top.txt", "Hello, World")
        other_fs.writetext("/foo/bar/baz/test.txt", "Goodbye, World")

        fs.move.move_dir(other_fs, "/", self.fs, self.prefix1+"/")

        expected = {self._prefix1+"/egg", self._prefix1+"/foo", self._prefix1+"/foo/bar", self._prefix1+"/foo/bar/baz"}
        self.assertEqual(other_fs.listdir("/"), [])
        self.assertEqual(set(walk.walk_dirs(self.fs, self.prefix1)), expected)
        self.assert_text("top.txt", "Hello, World")
        self.assert_text("/foo/bar/baz/test.txt", "Goodbye, World")

    def test_move_dir_mem(self):
        self._test_move_dir_write("mem://")

    def test_move_dir_temp(self):
        self._test_move_dir_write("temp://")

    def test_move_same_fs(self):
        self.fs.makedirs(self.prefix1+"foo/bar/baz")
        self.fs.makedir(self.prefix1+"egg")
        self.fs.writetext(self.prefix1+"top.txt", "Hello, World")
        self.fs.writetext(self.prefix1+"/foo/bar/baz/test.txt", "Goodbye, World")

        fs.move.move_dir(self.fs, self.prefix1+"foo", self.fs, self.prefix1+"foo2")

        expected = {self._prefix1+"/egg", self._prefix1+"/foo2", self._prefix1+"/foo2/bar", self._prefix1+"/foo2/bar/baz"}
        self.assertEqual(set(walk.walk_dirs(self.fs, self.prefix1)), expected)
        self.assert_text("top.txt", "Hello, World")
        self.assert_text("/foo2/bar/baz/test.txt", "Goodbye, World")

    def test_move_file_same_fs(self):
        text = "Hello, World"
        self.fs.makedir(self.prefix1+"foo").writetext("test.txt", text)
        self.assert_text("foo/test.txt", text)

        fs.move.move_file(self.fs, self.prefix1+"foo/test.txt", self.fs, self.prefix1+"foo/test2.txt")
        self.assert_not_exists("foo/test.txt")
        self.assert_text("foo/test2.txt", text)

    def _test_move_file(self, protocol):
        other_fs = open_fs(protocol)

        text = "Hello, World"
        self.fs.makedir(self.prefix1+"foo").writetext("test.txt", text)
        self.assert_text("foo/test.txt", text)

        with self.assertRaises(errors.ResourceNotFound):
            fs.move.move_file(self.fs, self.prefix1+"foo/test.txt", other_fs, self.prefix1+"foo/test2.txt")

        if self.prefix1 != '':
            other_fs.makedir(self.prefix1)
        other_fs.makedir(self.prefix1+"foo")

        fs.move.move_file(self.fs, self.prefix1+"foo/test.txt", other_fs, self.prefix1+"foo/test2.txt")

        self.assertEqual(other_fs.readtext(self.prefix1+"foo/test2.txt"), text)

    def test_move_file_mem(self):
        self._test_move_file("mem://")

    def test_move_file_temp(self):
        self._test_move_file("temp://")

    def test_copydir(self):
        self.fs.makedirs(self.prefix1+"foo/bar/baz/egg")
        self.fs.writetext(self.prefix1+"foo/bar/foofoo.txt", "Hello")
        self.fs.makedir(self.prefix1+"foo2")
        self.fs.copydir(self.prefix1+"foo/bar", self.prefix1+"foo2")
        self.assert_text("foo2/foofoo.txt", "Hello")
        self.assert_isdir("foo2/baz/egg")
        self.assert_text("foo/bar/foofoo.txt", "Hello")
        self.assert_isdir("foo/bar/baz/egg")

        with self.assertRaises(errors.ResourceNotFound):
            self.fs.copydir(self.prefix1+"foo", self.prefix1+"foofoo")
        with self.assertRaises(errors.ResourceNotFound):
            self.fs.copydir(self.prefix1+"spam", self.prefix1+"egg", create=True)
        with self.assertRaises(errors.DirectoryExpected):
            self.fs.copydir(self.prefix1+"foo2/foofoo.txt", self.prefix1+"foofoo.txt", create=True)

    def test_movedir(self):
        self.fs.makedirs(self.prefix1+"foo/bar/baz/egg")
        self.fs.writetext(self.prefix1+"foo/bar/foofoo.txt", "Hello")
        self.fs.makedir(self.prefix1+"foo2")
        self.fs.movedir(self.prefix1+"foo/bar", self.prefix1+"foo2")
        self.assert_text("foo2/foofoo.txt", "Hello")
        self.assert_isdir("foo2/baz/egg")
        self.assert_not_exists("foo/bar")
        self.assert_not_exists("foo/bar/foofoo.txt")
        self.assert_not_exists("foo/bar/baz/egg")

        # Check moving to an unexisting directory
        with self.assertRaises(errors.ResourceNotFound):
            self.fs.movedir(self.prefix1+"foo", self.prefix1+"foofoo")

        # Check moving an unexisting directory
        with self.assertRaises(errors.ResourceNotFound):
            self.fs.movedir(self.prefix1+"spam", self.prefix1+"egg", create=True)

        # Check moving a file
        with self.assertRaises(errors.DirectoryExpected):
            self.fs.movedir(self.prefix1+"foo2/foofoo.txt", self.prefix1+"foo2/baz/egg")

    def test_match(self):
        self.assertTrue(self.fs.match(["*.py"], "foo.py"))
        self.assertEqual(
            self.fs.match(["*.py"], "FOO.PY"),
            self.fs.getmeta().get("case_insensitive", False),
        )

    def test_tree(self):
        self.fs.makedirs(self.prefix1+"foo/bar")
        self.fs.create(self.prefix1+"test.txt")
        write_tree = io.StringIO()
        self.fs.tree(path=self.prefix1, file=write_tree)
        written = write_tree.getvalue()
        expected = "|-- foo\n|   `-- bar\n`-- test.txt\n"
        self.assertEqual(expected, written)

    def test_unicode_path(self):
        if not self.fs.getmeta().get("unicode_paths", False):
            raise unittest.SkipTest("the filesystem does not support unicode paths.")

        self.fs.makedir(self.prefix1+"földér")
        self.fs.writetext(self.prefix1+"☭.txt", "Smells like communism.")
        self.fs.writebytes(self.prefix1+"földér/☣.txt", b"Smells like an old syringe.")

        self.assert_isdir("földér")
        self.assertEqual(["☣.txt"], self.fs.listdir(self.prefix1+"földér"))
        self.assertEqual("☣.txt", self.fs.getinfo(self.prefix1+"földér/☣.txt").name)
        self.assert_text("☭.txt", "Smells like communism.")
        self.assert_bytes("földér/☣.txt", b"Smells like an old syringe.")

        if self.fs.hassyspath(self.prefix1+"földér/☣.txt"):
            self.assertTrue(os.path.exists(self.fs.getsyspath(self.prefix1+"földér/☣.txt")))

        self.fs.remove(self.prefix1+"földér/☣.txt")
        self.assert_not_exists("földér/☣.txt")
        self.fs.removedir(self.prefix1+"földér")
        self.assert_not_exists("földér")

    def test_case_sensitive(self):
        meta = self.fs.getmeta()
        if "case_insensitive" not in meta:
            raise unittest.SkipTest("case sensitivity not known")

        if meta.get("case_insensitive", False):
            raise unittest.SkipTest("the filesystem is not case sensitive.")

        self.fs.makedir(self.prefix1+"foo")
        self.fs.makedir(self.prefix1+"Foo")
        self.fs.touch(self.prefix1+"fOO")

        self.assert_exists("foo")
        self.assert_exists("Foo")
        self.assert_exists("fOO")
        self.assert_not_exists("FoO")

        self.assert_isdir("foo")
        self.assert_isdir("Foo")
        self.assert_isfile("fOO")

    def test_glob(self):
        self.assertIsInstance(self.fs.glob, glob.BoundGlobber)

    def test_hash(self):
        self.fs.makedir(self.prefix1+"foo").writebytes("hashme.txt", b"foobar" * 1024)
        self.assertEqual(
            self.fs.hash(self.prefix1+"foo/hashme.txt", "md5"), "9fff4bb103ab8ce4619064109c54cb9c"
        )
        with self.assertRaises(errors.UnsupportedHash):
            self.fs.hash(self.prefix1+"foo/hashme.txt", "nohash")

        with self.fs.opendir(self.prefix1+"foo") as foo_fs:
            self.assertEqual(
                foo_fs.hash("hashme.txt", "md5"), "9fff4bb103ab8ce4619064109c54cb9c"
            )

class TestS3FSWithPrefix(FSTestCasesWithPrefix, unittest.TestCase):
    """Test S3FS implementation from dir_path."""

    bucket_name = os.getenv("S3FS_TEST_BUCKET", "fsexample")
    prefix1 = bucket_name+'/'
    _prefix1 = bucket_name
    s3 = boto3.resource("s3")
    client = boto3.client("s3")

    def make_fs(self):
        self._delete_bucket_contents()
        return S3FSZero(None)

    def _delete_bucket_contents(self):
        response = self.client.list_objects(Bucket=self.bucket_name)
        contents = response.get("Contents", ())
        for obj in contents:
            self.client.delete_object(Bucket=self.bucket_name, Key=obj["Key"])


