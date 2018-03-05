from gensim import corpora
import os
from gensim import models
from collections import defaultdict
import json
from nltk.corpus import stopwords 
from nltk.stem.wordnet import WordNetLemmatizer
from string import punctuation
import datetime
from pprint import pprint  
import pickle

# deactivate parts of the training to save time
raw_corpus_exists = True
documents_cleaned = False
bow_corpus_exists = False
model_exists = False
# num_docs = float("inf")
# num_docs = 100000
num_docs = 1000000

raw_corpus = []
counter = 0
timestamp = str(datetime.datetime.now()).split('.')[0]

# Params   
iterations = 1000
num_topics = 100
alpha = 50/num_topics
eta = 0.1

print('Reading corpus from files...')
if not raw_corpus_exists:
    for filename in os.listdir("mailbodies_for_topic_model"):
        with open("mailbodies_for_topic_model/" + filename) as f:
            for line in f:
                counter = counter+1
                if counter == num_docs:
                    break
                elif counter % 10 == 0:
                    raw_corpus = raw_corpus + [json.loads(line)["parts"][0]["body"].replace(" EDRM Enron Email Data Set has been produced in EML, PST and NSF format by ZL Technologies, Inc This Data Set is licensed under a Creative Commons Attribution 3 0 United States License http creativecommons org licenses by 3 0 us To provide attribution, please cite to ZL Technologies, Inc http www zlti com", "")]
        if counter == num_docs:
            break
    with open('rawcorpus.p', 'wb') as pfile:
        pickle.dump(raw_corpus, pfile)
else:
    print('Reading corpus from pickled files...')
    with open('rawcorpus.p', 'rb') as pfile:
        raw_corpus = pickle.load(pfile)

print('---- Done reading')

if not documents_cleaned:
    # raw_corpus = ["Hello, I'm from Berlin and I have multiple dogs; in fact, I have 3 dogs. mp ee mvp",
    #               "This is ,.3. ;: a weird - string.",
    #               "I have 3,000 canary birds with an average accuracy of 0.673, 3 of them have an acc of over 0.92 today's.",
    #               "3,0 '2.9' top40companies enron"]

    # raw_corpus = ["I like to eat broccoli and bananas.",
    #               "I ate a banana and spinach smoothie for breakfast.",
    #               "Chinchillas and kittens are cute.",
    #               "My sister adopted a kitten and hamsters yesterday.",
    #               "Look at this cute hamster munching on a piece of broccoli."]

    print('Cleaning documents...')
    stopwords = stopwords.words('english')

    lemma = WordNetLemmatizer()

    short_tokens = set()
    numbers = set()

    def clean(doc):
        tokens = [token for token in doc.lower().split()]
        # print('tokens\n', tokens)
        punc_free = [token.strip(punctuation) for token in tokens]
        # print('punc_free\n', punc_free)
        empty_string_free = [token for token in punc_free if token]
        # print('empty_string_free\n', empty_string_free)
        stopword_free = [word for word in empty_string_free if word not in stopwords]
        # print('short word free\n', short worde free) 
        short_word_free = [word for word in stopword_free if len(word) > 3]
        # print('stopword_free\n', stopword_free)
        short_token_free = [word if len(word) > 2 else short_tokens.add(word) for word in stopword_free]
        # print('short_token_free\n', short_token_free)
        empty_string_free2 = [token for token in short_token_free if token]
        # print('empty_string_free2\n', empty_string_free2)
        numerics_free = []
        for token in empty_string_free2:
            if [char for char in token if not (char.isdigit() or char in punctuation)]:
                numerics_free.append(token)
            else:
                numerics_free.append('lt_number')
                numbers.add(token)
        # print('numerics_free\n', numerics_free)
        lemmatized = [lemma.lemmatize(word) for word in numerics_free]
        # print('lemmatized\n', lemmatized)
        return lemmatized

    docs = [clean(doc) for doc in raw_corpus]
    print('short tokens', short_tokens)
    print('numbers', numbers)
    # docs = [clean(doc).split() for doc in raw_corpus if len(doc.split()) > 200]

    # print('count overall word frequencies')
    # word_freq = defaultdict(int)
    # for doc in docs:
    #     for token in doc:
    #         word_freq[token] += 1
    # pprint(word_freq)

    print('count number of docs a word appears in')
    word_doc_appearances = defaultdict(set)
    for i, doc in enumerate(docs):
        for token in doc:
            word_doc_appearances[token].add(i)

    high_freq_tokens = set()
    low_freq_tokens = set()

    MIN_FREQ = 3
    # MIN_FREQ = 1
    MAX_PERCENTAGE = 0.05
    # MAX_PERCENTAGE = 1.0
    max_freq = MAX_PERCENTAGE * len(docs)
    def filter_by_freq(doc):
        filtered_doc = []
        for token in doc:
            if token == 'lt_number':
                filtered_doc.append(token)
            elif len(word_doc_appearances[token]) < MIN_FREQ:
                low_freq_tokens.add(token)
            elif len(word_doc_appearances[token]) > max_freq:
                high_freq_tokens.add(token)
            else:
                filtered_doc.append(token)
        return filtered_doc

    print('only keep words that appear in more than or equal to ' + str(MIN_FREQ) + ' docs')
    print('only keep words that appear in less than or equal to ' + str(MAX_PERCENTAGE) +'% of docs')
    docs = [filter_by_freq(doc) for doc in docs]

    print('high_freq_tokens', high_freq_tokens)
    print('low_freq_tokens', low_freq_tokens)

    # sys.exit()

    print('remove docs that became empty because of frequency constraints')
    docs = [doc for doc in docs if doc]

    print('corpus preprocessing done')

    processed_corpus = docs
    with open('cleaned_documents.p', 'wb') as pfile:
        pickle.dump(raw_corpus, pfile)

else:
    with open('cleaned_documents.p', 'rb') as pfile:
        processed_corpus = pickle.load(pfile)
        
    
if not bow_corpus_exists:
    print("save dictionary and bow corpus_______________")
    dictionary = corpora.Dictionary(processed_corpus)
    dictionary.save("lda.dictionary_" + timestamp)
    bow_corpus = [dictionary.doc2bow(text) for text in processed_corpus]
    with open('bowcorpus_' + timestamp + '.p', 'wb') as pfile:
        pickle.dump(bow_corpus, pfile)

else:
    print("load dictionary and bow corpus_______________")
    dictionary = corpora.Dictionary.load("lda.dictionary")
    with open('bowcorpus.p', 'rb') as pfile:
        bow_corpus = pickle.load(pfile)


# (save and train) or load model
if not model_exists:
    print("train model_______________")
    lda = models.ldamodel.LdaModel(bow_corpus, num_topics=100, iterations=iterations, eta=eta, alpha=alpha)
    lda.save("lda.model")

else:    
    print("load model_______________")
    lda = models.LdaModel.load('lda.model')


print("log training data_______________")
# write log file
with open("result_logs/" + timestamp + ".txt", "a") as f:
    f.write("META" + "\n")
    f.write("vocabulary size:" + "\n")
    f.write(str(len(tokens_for_counting)) + "tokens" + "\n")
    f.write("PARAMS" + "\n")
    f.write("iterations:" + str(iterations) + "\n")
    f.write("alpha:" + str(alpha) + "\n")
    f.write("eta:" + str(eta) + "\n")
    f.write("number of topics:" + str(num_topics) + "\n")
    f.write("FILTERED" + "\n")
    f.write("stopwords: " + "\n")
    f.write(str(stopwords) + "\n")
    f.write("words that are too frequent" + "\n")
    f.write(str(high_freq_tokens)+ "\n")
    f.write("words that are too infrequent" + "\n")
    f.write(str(low_freq_tokens)+ "\n")
    f.write(str(stopwords)+ "\n")
    f.write("TOPICS" + "\n")
    f.write("topics" + "\n")
    for topic in lda.show_topics(num_topics=20, num_words=5, log=False, formatted=True):
        f.write(str((list(map(lambda xy: (dictionary[xy[0]], xy[1]), lda.get_topic_terms(topic[0], topn=10))))) + "\n")


print("test topics on sample documents_______________")

sample_docs = [
    "Gerald attached below is a P A for an open season we participated in off \r Williams Gas Pipeline I doubt that we will actually take capacity however, \r Williams is allowing a management out Therefore, it represents a free \r option Please review and work your magic \r Mark I ve also attached Williams presentation on the project for your use \r if any \r Thanks\r chris\r x31666\r AM \r Sanders, Dale T Dale T Sanders Williams com on 11 01 2000 09 11 45 AM\r Chris, \r Attached is a presentation and precedent agreement that we have developed \r after evaluating the results of our non binding open season We would like \r to set up a conference call with you to discuss these developments.",
    "This is a notice to inform you that the server where your Outlook mailbox resides is scheduled for an outage tonight Your mailbox will be temporarily unavailable starting anytime after 11pm and may continue to be unavailable until 1 a m , when all server maintenance work have been completed Outlook Web Access OWA will also be unavailable during this time \r Blackberry users Message delivery may be delayed for a few minutes \r If you have any questions, please call the Resolution Center",
    "Hey man!  Haven't talked to you in awhile.  Hope things are good.  David\r\nreminded me about the game the other day.  Are you still headed to Spain\r\nfor Thanksgiving?  If so, let me know when you want to get together and do\r\nthe ticket/money swap.  I'll be in Florida this weekend, but should be home\r\non weekends after that for awhile.  Or, we can do it one week night if I'm\r\nin town.  Unfortunately, my travel schedule is not slowing down around the\r\nholidays like I'd hoped.\r\n\r\nI also have another favor to ask - is there any chance you would go to our\r\nChristmas party with me on Dec 8 if I go?  David will obviously be there,\r\ntoo.  I'm such a loser that I've had to go by myself the last three years\r\nand that's such a drag.  Let me know if you think you'd consider it.",
    "Ryan's big birthday is coming up this Tuesday, March 27th...wanted to get\r\n>some people together for a nice dinner downtown, around 7:00.  Please feel\r\n>free to bring Barbara along too!  I still need to make reservations at a\r\n>restaurant..if you can think of any place great, let me know.",
    "hen a dashing young sailor is betrayed by his best friend and unjustly imprisoned, his peaceful life is shattered and his beautiful fiancée stolen away Consumed by thoughts of vengeance, he escapes from his hellish prison, transforms himself into a mysterious and wealthy French nobleman, and exacts revenge on all who wronged him Alexandre Dumas s classic swashbuckling story gets the royal treatment in The Count of Monte Cristo"
]

for doc in sample_docs:
    print(lda.get_document_topics(dictionary.doc2bow(doc)))

