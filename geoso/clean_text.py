import re
import string
import pycountry
import spacy
# from nltk.corpus import wordnet
from spacy.lang.xx import MultiLanguage
from spacy.lang.en import English
import pandas as pd

punct = list(string.punctuation)
extended_stop_words = ['rt', 'via', 'http', 'https', '...']


class TextCleaner:
    def __init__(self):
        pass

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
                # spacy.load("en_core_web_sm")
                TextCleaner.nlp_for_languages['en'] = English()
                TextCleaner.stopwords_for_languages['en'] = spacy.lang.en.stop_words.STOP_WORDS
            elif lang in TextCleaner.supported_languages:
                # In order to use languages that don’t yet come with a model, you have to import them directly, or use spacy.blank:
                TextCleaner.nlp_for_languages[lang] = spacy.blank(lang)
                TextCleaner.stopwords_for_languages[lang] = getattr(
                    spacy.lang, lang).stop_words.STOP_WORDS
                # if lang == 'sv':
                #     TextCleaner.stopwords_for_languages[lang] = spacy.lang.sv.stop_words.STOP_WORDS
                # elif lang == 'ar':
                #     TextCleaner.stopwords_for_languages[lang] = spacy.lang.ar.stop_words.STOP_WORDS
        return TextCleaner.nlp_for_languages[lang], TextCleaner.stopwords_for_languages[lang]

    @staticmethod
    def _tokenize_lemmatize(text, lang, lemmatize=False, remove_stop_words=False, remove_url=True):
        if not lang in TextCleaner.supported_languages:
            return []
        nlp, stop_words = TextCleaner.get_nlp_and_stopwords(lang)
        doc = nlp(text)
        # for word in doc:
        #     if not word.is_stop:
        #         print("{}\t:{}".format(word, word.lemma_))
        res = None

        if lemmatize and remove_stop_words:
            res = [
                word.lemma_ for word in doc if not word.is_stop and not word.is_punct]
        elif lemmatize and not remove_stop_words:
            res = [word.lemma_ for word in doc if not word.is_punct]
        elif not lemmatize and remove_stop_words:
            res = [word for word in doc if not word.is_stop and not word.is_punct]
        else:
            res = [word for word in doc if not word.is_punct]

        if remove_stop_words:
            res = [word for word in res if word not in extended_stop_words]

        if remove_url:
            res = [word for word in res if not word.like_url]

        res = [word.text for word in res]

        return " ".join(res)

    @ staticmethod
    def normalize_text(text: str):
        text = TextCleaner._remove_usernames(text)
        text = text.replace('-', ' ')
        # normalization 1: xxxThis is a --> xxx. This is a (missing delimiter)
        text = re.sub(r'([a-z])([A-Z])', r'\1\. \2', text)  # before lower case
        # normalization 2: lower case
        text = text.lower()
        text = TextCleaner._remove_url(text)        
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

    @ staticmethod
    def _remove_white_space(text):
        # correct all multiple white spaces to a single white space
        t1 = re.sub(r'[\s]+', ' ', text)
        # # Additional clean up : removing words less than 3 chars, and remove space at the beginning and teh end
        # t2 = re.sub(r'\W*\b\w{1,3}\b', '', t1)
        return t1.strip()

    @ staticmethod
    def _replace_hashtag_by_text(text):
        return re.sub(r'#([^\s]+)', r'\1', text)

    # @staticmethod
    # def _remove_repeated_characters(tokens):
    #     if tokens is None:
    #         return None
    #     repeat_pattern = re.compile(r'(\w*)(\w)\2(\w*)')

    #     match_substitution = r'\1\2\3'

    #     def replace(old_word):
    #         if wordnet.synsets(old_word):
    #             return old_word

    #         new_word = repeat_pattern.sub(match_substitution, old_word)
    #         return replace(new_word) if new_word != old_word else new_word

    #     correct_tokens = [replace(word) for word in tokens]
    #     return correct_tokens

    @ staticmethod
    def _remove_punct(text):
        text = "".join(
            [char for char in text if char not in string.punctuation])
        text = re.sub('[0-9]+', '', text)
        return text

    @ staticmethod
    def _remove_usernames(text):
        return re.sub(r'@[^\s]+', '', text)

    @ staticmethod
    def _remove_atsign(text):
        return re.sub(r'@[A-Za-z0-9]+', '', text)

    @ staticmethod
    def _remove_url(text):
        return re.sub(r'((www\.[^\s]+)|(https?://[^\s]+))', '', text)
        # return re.sub(r"http\S+", "", text)
        # return re.sub('https?://[A-Za-z0-9./]+', '', text)
        # convert url to string
        # return re.sub(r'\w+:\/{2}[\d\w-]+(\.[\d\w-]+)*(?:(?:\/[^\s/]*))*', '', text)

    @ staticmethod
    def _remove_utf8_bom(text: str):
        # remove UTF-8 BOM (Byte Order Mark)
        # https://towardsdatascience.com/another-twitter-sentiment-analysis-bb5b01ebad90
        return text.replace(u"\ufffd", "?")

    @ staticmethod
    def _remove_hashtag_numbers(text):
        return re.sub("[^a-zA-Z]", " ", text)


def twitter_clean_text(text, lang_code: str = '', lang_full_name: str = '',
                       lemmatize: bool = False, remove_url: bool = True,
                       return_empty_for_not_supported_lang: bool = True):
    """Clean the text of a tweet

    Args:
        text: The text of a tweet or a pandas.DataFrame containing the texts of several tweets
        lang_code (str, optional): The language code of the tweet. Either lang_code or lang_full_name must be provided.
        lang_full_name (str, optional): The language of the tweet. Either lang_code or lang_full_name must be provided.
        lemmatize (bool, optional): Apply lemmatization on the next. Defaults to False.
        remove_url (bool, optional): Remove url. Defaults to True.
        return_empty_for_not_supported_lang (bool, optional):        
    Raises:
        ValueError: Either lang_code or lang_full_name must be provided. Also, the language could be not supported.

    Returns:
        str: Cleaned text of the tweet.
    """

    if lang_full_name == '' and lang_code == '':
        raise ValueError("Either lang or lang_full_name must be provided.")

    try:
        if lang_full_name == '':
            lang_full_name = str.lower(
                pycountry.languages.get(alpha_2=lang_code).name)
        if lang_code == '':
            lang = str.lower(pycountry.languages.get(name=lang_full_name).name)
    except:
        if return_empty_for_not_supported_lang:
            return ''
        else:
            raise ValueError(
                f"The language is not supported (lang_code: {lang_code}, lang_full_name: {lang_full_name}).")
    if not lang_code in TextCleaner.supported_languages:
        if return_empty_for_not_supported_lang:
            return ''
        else:
            raise ValueError(
                f"The language is not supported (lang_code: {lang_code}, lang_full_name: {lang_full_name}).")

    # https://towardsdatascience.com/another-twitter-sentiment-analysis-bb5b01ebad90
    text = text.replace('\\/', '/')
    text = TextCleaner._tokenize_lemmatize(
        text=text, lang=lang_code,
        lemmatize=lemmatize,
        remove_url=remove_url)
    text = TextCleaner.normalize_text(text)

    return text


def twitter_clean_text_in_dataframe(df: pd.DataFrame,
                                    text_column: str = 'text',
                                    lang_code_column: str = 'lang',
                                    lemmatize: bool = False,
                                    remove_url: bool = True,
                                    return_empty_for_not_supported_lang: bool = True):
    """Clean the text of tweets

    Args:
        df (pandas.DataFrame): A pandas.DataFrame containing the texts of several tweets
        text_column (str, optional): The name of column containing the texts of tweets, if `input` is a pandas.DataFrame. Defaults to text.
        lang_code_column (str, optional): The name of column containing the language code of the tweet. Defaults to lang.
        lemmatize (bool, optional): Apply lemmatization on the next. Defaults to False.
        remove_url (bool, optional): Remove url. Defaults to True.
        return_empty_for_not_supported_lang (bool, optional):

    Raises:
        ValueError: Either lang_code or lang_full_name must be provided. Also, the language could be not supported.

    Returns:
        str or pandas.DataFrame: Cleaned text of the tweet or a pandas.DataFrame containing the cleaned text of tweets.
    """
    return df.apply(lambda x: twitter_clean_text(text=x.get(text_column),
                                                 lang_code=x.get(lang_code_column),
                                                 lemmatize=lemmatize, 
                                                 remove_url=remove_url, 
                                                 return_empty_for_not_supported_lang=return_empty_for_not_supported_lang), axis=1)
