import re
import string
import pycountry
import spacy
from nltk.corpus import wordnet
from spacy.lang.xx import MultiLanguage
from spacy.lang.en import English

punct = list(string.punctuation)
extended_stop_words = ['rt', 'via', 'http', 'https', '...']


class TextCleaner:
    def __init__(self):
        pass

    @staticmethod
    def clean_text(text: str, lang: str = '', lang_full_name: str = ''):

        if lang_full_name == '' and lang == '':
            raise ValueError("Either lang or lang_full_name must be provided.")
        try:
            if lang_full_name == '':
                lang_full_name = str.lower(pycountry.languages.get(alpha_2=lang).name)
            if lang == '':
                lang = str.lower(pycountry.languages.get(name=lang_full_name).name)
        except:
            print(lang)
        # https://towardsdatascience.com/another-twitter-sentiment-analysis-bb5b01ebad90
        text = TextCleaner.normalize_text(text)
        lam_of_tweet = TextCleaner._tokenize_lemmatize(text=text, lang=lang)
        lam_of_tweet_repeated_removed = TextCleaner._remove_repeated_characters(lam_of_tweet)
        lam = " ".join(lam_of_tweet_repeated_removed)
        num_of_words = len(lam_of_tweet_repeated_removed)

        return lam, num_of_words, lang_full_name

        # Excluded languages: ja, uk, th, 'vi', 'yo', 'zh', 'tl', 'ta', 'te, 'mr', 'si', 'kn', 'ko', 'id', 'eu'



    supported_languages = ['de', 'el', 'en', 'es', 'fr', 'it', 'lt', 'nb', 'nl', 'pt', 'xx', 'af', 'ar', 'bg', 'bn',
                           'ca', 'cs', 'da', 'et', 'fa', 'fi', 'ga', 'he', 'hi', 'hr', 'hu', 'is', 'lb', 'lv',
                           'pl', 'ro', 'ru', 'sk', 'sl', 'sq', 'sr', 'sv', 'tr', 'tt', 'ur']

    stopwords_for_languages = {}
    nlp_for_languages = {}

    @staticmethod
    def is_lang_supported(lang):
        return lang in TextCleaner.supported_languages

    @staticmethod
    def get_nlp_and_stopwords(lang):
        if not lang in TextCleaner.nlp_for_languages:
            if lang == 'en':
                TextCleaner.nlp_for_languages['en'] = English() #spacy.load("en_core_web_sm")
                TextCleaner.stopwords_for_languages['en'] = spacy.lang.en.stop_words.STOP_WORDS
            elif lang in TextCleaner.supported_languages:
                # In order to use languages that don’t yet come with a model, you have to import them directly, or use spacy.blank:
                TextCleaner.nlp_for_languages[lang] = spacy.blank(lang)
                TextCleaner.stopwords_for_languages[lang] = getattr(spacy.lang, lang).stop_words.STOP_WORDS
                # if lang == 'sv':
                #     TextCleaner.stopwords_for_languages[lang] = spacy.lang.sv.stop_words.STOP_WORDS
                # elif lang == 'ar':
                #     TextCleaner.stopwords_for_languages[lang] = spacy.lang.ar.stop_words.STOP_WORDS
        return TextCleaner.nlp_for_languages[lang], TextCleaner.stopwords_for_languages[lang]

    @staticmethod
    def _tokenize_lemmatize(text, lang):
        if not lang in TextCleaner.supported_languages:
            return []
        nlp, stop_words = TextCleaner.get_nlp_and_stopwords(lang)
        doc = nlp(text)
        # for word in doc:
        #     if not word.is_stop:
        #         print("{}\t:{}".format(word, word.lemma_))
        res = [word.lemma_ for word in doc if not word.is_stop]
        res = [word for word in res if word not in punct]
        res = [word for word in res if word not in extended_stop_words]
        return res

    @staticmethod
    def normalize_text(text):
        text = text.replace('-', ' ')
        # normalization 1: xxxThis is a --> xxx. This is a (missing delimiter)
        text = re.sub(r'([a-z])([A-Z])', r'\1\. \2', text)  # before lower case
        # normalization 2: lower case
        text = text.lower()
        text = TextCleaner._remove_url(text)
        text = TextCleaner._remove_usernames(text)
        text = TextCleaner._remove_punct(text)
        text = TextCleaner._replace_hashtag_by_text(text)
        text = TextCleaner._remove_white_space(text)
        text = TextCleaner._remove_utf8_bom(text)
        # normalization 3: "&gt", "&lt"
        text = re.sub(r'&gt|&lt', ' ', text)
        # normalization 4: letter repetition (if more than 2)
        text = re.sub(r'([a-z])\1{2,}', r'\1', text)
        # normalization 5: non-word repetition (if more than 1)
        text = re.sub(r'([\W+])\1{1,}', r'\1', text)
        # normalization 6: string * as delimiter
        text = re.sub(r'\*|\W\*|\*\W', '. ', text)
        # normalization 7: stuff in parenthesis, assumed to be less informal
        text = re.sub(r'\(.*?\)', '. ', text)
        # normalization 8: xxx[?!]. -- > xxx.
        text = re.sub(r'\W+?\.', '.', text)
        # normalization 9: [.?!] --> [.?!] xxx
        text = re.sub(r'(\.|\?|!)(\w)', r'\1 \2', text)
        # normalization 10: ' ing ', noise text
        # text = re.sub(r' ing ', ' ', text)
        # normalization 11: noise text
        text = re.sub(r'product received for free[.| ]', ' ', text)
        # normalization 12: phrase repetition
        text = re.sub(r'(.{2,}?)\1{1,}', r'\1', text)
        return text

    @staticmethod
    def _remove_white_space(text):
        # correct all multiple white spaces to a single white space
        t1 = re.sub(r'[\s]+', ' ', text)
        # Additional clean up : removing words less than 3 chars, and remove space at the beginning and teh end
        t2 = re.sub(r'\W*\b\w{1,3}\b', '', t1)
        return t2.strip()

    @staticmethod
    def _replace_hashtag_by_text(text):
        return re.sub(r'#([^\s]+)', r'\1', text)

    @staticmethod
    def _remove_repeated_characters(tokens):
        if tokens is None:
            return None
        repeat_pattern = re.compile(r'(\w*)(\w)\2(\w*)')

        match_substitution = r'\1\2\3'

        def replace(old_word):
            if wordnet.synsets(old_word):
                return old_word

            new_word = repeat_pattern.sub(match_substitution, old_word)
            return replace(new_word) if new_word != old_word else new_word

        correct_tokens = [replace(word) for word in tokens]
        return correct_tokens

    @staticmethod
    def _remove_punct(text):
        text = "".join([char for char in text if char not in string.punctuation])
        text = re.sub('[0-9]+', '', text)
        return text

    @staticmethod
    def _remove_usernames(text):
        return re.sub(r'@[^\s]+', '', text)

    @staticmethod
    def _remove_atsign(text):
        return re.sub(r'@[A-Za-z0-9]+', '', text)

    @staticmethod
    def _remove_url(text):
        return re.sub(r'((www\.[^\s]+)|(https?://[^\s]+))', '', text)
        # return re.sub(r"http\S+", "", text)
        # return re.sub('https?://[A-Za-z0-9./]+', '', text)
        # convert url to string
        # return re.sub(r'\w+:\/{2}[\d\w-]+(\.[\d\w-]+)*(?:(?:\/[^\s/]*))*', '', text)

    @staticmethod
    def _remove_utf8_bom(text: str):
        # remove UTF-8 BOM (Byte Order Mark)
        # https://towardsdatascience.com/another-twitter-sentiment-analysis-bb5b01ebad90
        return text.replace(u"\ufffd", "?")

    @staticmethod
    def _remove_hashtag_numbers(text):
        return re.sub("[^a-zA-Z]", " ", text)
