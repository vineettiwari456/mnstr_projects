import nltk, spacy
# nltk.download('stopwords')
import gensim
import gensim.corpora as corpora
from gensim.utils import simple_preprocess
from gensim.models import CoherenceModel
import re
import numpy as np
import pandas as pd
from pprint import pprint
from nltk.corpus import stopwords

stop_words = stopwords.words('english')
stop_words.extend(['from', 'subject', 're', 'edu', 'use'])
data = [
    "From surviving to thriving: Reimagining the post-COVID-19 return For many, the toughest leadership test is now looming: how to bring a business back in an environment where a vaccine has yet to be found and economies are still reeling. by Kevin Sneader and Bob Sternfels May 2020 © Maskot/Getty Images The 1966 World Cup marked a low point for Brazilian soccer. Although the winner of the previous two tournaments, the team was eliminated in the first round, and its star player, Pelé, failed to perform. Fouled frequently and flagrantly, he threatened never to return to the World Cup. Many wondered if Brazil’s glory days were over. Four years later, however, Brazil won again, with such grace and style that the 1970 team is not only widely regarded as the best team ever to take the pitch but also as the most beautiful. And Pelé was named the player of the tournament. Making this turnaround required innovation, in particular, the creation of a unique attacking style of soccer. It required building a cohesive team, even as most of the roster changed. And it required leadership, both in management and on the field. The result: by reimagining everything, Brazil came back stronger. As businesses around the world consider how they can return from the torment inflicted by the coronavirus, Brazil’s journey from failure to triumph provides food for thought. In a previous article, McKinsey described five qualities that will be critical for business leaders to find their way to the next normal: resolve, resilience, return, reimagination, and reform. We noted that there would likely be overlap among these stages, and the order might differ, depending on the business, the sector, and the country. In this article, we suggest that in order to come back stronger, companies should reimagine their business model as they return to full speed. The moment is not to be lost: those who step up their game will be better off and far more ready to confront the challenges—and opportunities—of the next normal than those who do not. There are four strategic areas to focus on: recovering revenue, rebuilding operations, rethinking the organization, and accelerating the adoption of digital solutions. 1.Rapidly recover revenue. Speed matters: it will not be enough for companies to recover revenues gradually as the crisis abates. They will need to fundamentally rethink their revenue profile, to position themselves for the long term and to get ahead of the competition. To do this companies must SHAPE up. Start-up mindset. This favors action over research, and testing over analysis. Establish a brisk cadence to encourage agility and accountability: daily team check-ins, weekly 30-minute CEO reviews, and twice-a-month 60-minute reviews. Human at the core. Companies will need to rethink their operating model based on how their people work best. Sixty percent of businesses surveyed by McKinsey in early April said that their new remote sales models were proving as much (29 percent) or more effective (31 percent) than traditional channels. Acceleration of digital, tech, and analytics. It’s already a cliché: the COVID-19 crisis has accelerated the shift to digital. But the best companies are going further, by enhancing and expanding their digital channels. They’re successfully using advanced analytics to combine new sources of data, such as satellite imaging, with their own insights to make better and faster decisions and strengthen their links to customers. Purpose-driven customer playbook. Companies need to understand what customers will value, post-COVID-19, and develop new use cases and tailored experiences based on those insights. Ecosystems and adaptability. Given crisis- related disruptions in supply chains and channels, adaptability is essential. That will mean changing the ecosystem and considering nontraditional collaborations with partners up and down the supply chain. 2 From surviving to thriving: Reimagining the post-COVID-19 return Rapid revenue response isn’t just a way to survive the crisis. It’s the next normal for how companies will have to operate. Assuming company leaders are in good SHAPE, how do they go about choosing what to do? We see three steps. Identify and prioritize revenue opportunities. What’s important is to identify the primary sources of revenue and, on that basis, make the “now or never” moves that need to happen before the recovery fully starts. This may include launching targeted campaigns to win back loyal customers; developing customer experiences focused on GES 2020 increased health and safety; adjusting pricing COVID Reimagining Return and promotions based on new data; reallocating Exhibit 1 of 1 spending to proven growth sources; reskilling the sales force to support remote selling; creating flexible payment terms; digitizing sales channels; and automating processes to free up sales representatives to sell more. Once identified, these measures need to be rigorously prioritized to reflect their impact on earnings and the company’s ability to execute quickly (exhibit). Act with urgency. During the current crisis, businesses have worked faster and better than they dreamed possible just a few months ago. Maintaining that sense of possibility will be an enduring source of competitive advantage. Consider a Chinese car-rental company whose revenues fell 95 percent in February. With the roads Exhibit Recovering revenues is an important element of reimagining the return. Matrix for prioritizing measures for rapid revenue recovery, illustrative HIGH Structural shifts Short-term moves Sales restructuring M&A moves Revenue-growth management E-commerce analytics Customer experience, safety Marketing efficiency E-commerce features Assortment, range, sizing, packaging Demand planning Impact on earnings before interest and taxes LOW SLOW Time to impact FAST 3 From surviving to thriving: Reimagining the post-COVID-19 return During the current crisis, businesses have worked faster and better than they dreamed possible just a few months ago. Maintaining that sense of possibility will be an enduring source of competitive advantage. empty, company leaders didn’t just stew. Instead, they reacted like a start-up. They invested in micro–customer segmentation and social listening to guide personalization. This led them to develop new use cases. They discovered, for example, that many tech firms were telling employees not to use public transportation. The car-rental company used this insight to experiment with and refine targeted campaigns. They also called first-time customers who had cancelled orders to reassure them of the various safety steps the company had taken, such as “no touch” car pickup. To manage the program, they pulled together three agile teams with cross- functional skills and designed a recovery dashboard to track progress. Before the crisis, the company took up to three weeks to launch a campaign; that is now down to two to three days. Within seven weeks, the company had recovered 90 percent of its business, year on year—almost twice the rate of its chief competitor. Develop an agile operating model. Driven by urgency, marketing and sales leaders are increasingly willing to embrace agile methods; they are getting used to jumping on quick videoconferences to solve problems and give remote teams more decision- making authority. It’s also important, of course, for cross-functional teams not to lose sight of the long term and to avoid panic reactions. In this sense, “agile” means putting in place a new operating model built around the customer and supported by the right processes and governance. Agile sales organizations, for example, continuously prioritize accounts and deals, and decide quickly where to invest. But this is effective only if there is a clear growth plan that sets out how to win each type of customer. Similarly, fast decision making between local sales and global business units and the rapid reallocation of resources between them require a stable sales-pipeline-management process. 2. Rebuilding operations. The coronavirus pandemic has radically changed demand patterns for products and services across sectors, while exposing points of fragility in global supply chains and service networks. At the same time, it has been striking how fast many companies have adapted, creating radical new levels of visibility, agility, productivity, and end- customer connectivity. Now leaders are asking themselves: How can we sustain this performance? As operations leaders seek to reinvent the way they work and thus position themselves for the next normal, five themes are emerging. Building operations resilience. Successful companies will redesign their operations and supply chains to protect against a wider and more acute range of potential shocks. In addition, they will act quickly to rebalance their global asset base and supplier mix. The once-prevalent global-sourcing model in product-driven value chains has steadily 4 From surviving to thriving: Reimagining the post-COVID-19 return declined as new technologies and consumer- demand patterns encourage regionalization of supply chains. We expect this trend to accelerate. This reinvention and regionalization of global value chains is also likely to accelerate adoption of other levers to strengthen operational resilience, including increased use of external suppliers to supplement internal operations, greater workforce cross-training, and dual or even triple sourcing. Accelerating end-to-end value-chain digitization. Creating this new level of operations resilience could be expensive, in both time and resources. The good news, however, is that leading innovators have demonstrated how “Industry 4.0” (or the Fourth Industrial Revolution suite of digital and analytics tools and approaches) can significantly reduce the cost of flexibility. In short, low-cost, high- flexibility operations are not only possible—they are happening. Most companies were already digitizing their operations before the coronavirus hit. If they accelerate these efforts now, they will likely see significant benefits in productivity, flexibility, quality, and end-customer connectivity. Rapidly increasing capital- and operating-expense transparency. To survive and thrive amid the economic fallout, companies can build their next- normal operations around a revamped approach to spending. A full suite of technology-enabled methodologies is accelerating cost transparency, compressing months of effort into weeks or days. These digital approaches include procurement- spend analysis and clean-sheeting, end-to- end inventory rebalancing, and capital-spend diagnostics and portfolio rationalization. Companies are also seeking to turn fixed capital costs into variable ones by leveraging “as a service” models. Embracing the future of work. The future of work, defined by the use of more automation and technology, was always coming. COVID-19 has hastened the pace. Employees across all functions, for example, have learned how to complete tasks remotely, using digital communication and collaboration tools. In operations, changes will go further, with an accelerated decline in manual and repetitive tasks and a rise in the need for analytical and technical support. This shift will call for substantial investment in workforce engagement and training in new skills, much of it delivered using digital tools. Reimagining a sustainable operations competitive advantage. Dramatic shifts in industry structure, customer expectations, and demand patterns create a need for equally dramatic shifts in operations strategies to create competitive advantage and new customer value propositions. Successful companies will reinvent the role of operations in their enterprises, creating new value through a far greater responsiveness to their end customers—including but not limited to accelerated product-development and customer- experience innovation, mass customization, improved environmental sustainability, and more interconnected, nimble ecosystem management. Taking action. To keep up during COVID-19, companies have moved fast. Sales and operation planning used to be done weekly or even monthly; now a daily cadence is common. To build on this progress, speed will continue to be of the essence. Companies that recognize this, and that are willing to set new standards and upend old paradigms, will build long-term strategic advantage. 3. Rethinking the organization. In 2019, a leading retailer was exploring how to launch a curbside-delivery business; the plan stretched over 18 months. When the COVID-19 lockdown hit the United States, it went live in two days. There are many more examples of this kind. “How can we ever tell ourselves that we can’t be faster?” one executive of a consumer company recently asked. Call it the “great unfreezing”: in the heat of the coronavirus crisis, organizations have been forced to work in new ways, and they are responding. Much of this progress comes from shifts in operating models. Clear goals, focused teams, and rapid decision making have replaced corporate bureaucracy. Now, as the world begins to move into the post-COVID-19 5 From surviving to thriving: Reimagining the post-COVID-19 return era, leaders must commit to not going back. The way in which they rethink their organizations will go a long way in determining their long-term competitive advantage. Specifically, they must decide who they are, how to work, and how to grow. Who we are. In a crisis, what matters becomes very clear, very fast. Strategy, roles, personal ownership, external orientation, and leadership that is both supportive and demanding—all can be seen much more clearly now. The social contract between the employee and employer is, we believe, changing fundamentally. “It will matter whether you actually acted to put the safety of employees and communities first,” one CEO told us, “or just said you cared.” One noticeable characteristic of companies that have adapted well is that they have a strong sense of identity. Leaders and employees have a shared sense of purpose and a common performance culture; they know what the company stands for, beyond shareholder value, and how to get things done right. a dynamic network of teams is more effective. They are rewiring their circuits to make decisions faster, and with much less data and certainty than before. In a world where fast beats slow, companies that can institutionalize these forms of speedy and effective decentralization will jump ahead of the competition. Organizations are also showing a more profound appreciation for matching the right talent, regardless of hierarchy, to the most critical challenges. In an environment with strong cost pressures, successful leaders will see the value in continuing to simplify and streamline their organizational structures. Experience has shown a better way, with critical roles linked to value- creation opportunities and leadership roles that are much more fluid, with new leaders emerging from unexpected places: the premium is placed on character and results, rather than on expertise or experience. This can only work, however, if the talent is there. To hire and keep top talent, the scarcest capital of all, means creating a unique work experience and committing to a renewed emphasis on talent development. How we work. Many leaders are reflecting on how small, nimble teams built in a hurry to deal with the COVID-19 emergency made important decisions faster and better. What companies have learned cannot be unlearned—namely, that a flatter organization that delegates decision making down to How to grow. Coming out of the crisis, organizations must answer important questions about growth and scalability. Three factors will matter most: the ability to embed data and analytics in decision making; the creation of learning platforms that support both individual and institutional experimentation Many leaders are reflecting on how small, nimble teams built in a hurry to deal with the COVID-19 emergency made important decisions faster and better. 6 From surviving to thriving: Reimagining the post-COVID-19 return and learning at scale; and the cultivation of an organizational culture that fosters value creation with other partners. Those organizations that are making the shift from closed systems and one-to-one transactional relationships to digital platforms and networks of mutually beneficial partnerships have proved more resilient during the crisis. “Every business is now a technology business, and what matters most is a deep understanding of the customer, which is enabled by technology,” remarked a retail CEO. By organizing to encourage insight generation—for example, by linking previously unconnected goods and services—technology is revolutionizing how organizations relate to their customers and their customers’ customers. Creating digitally enabled ecosystems is therefore critical because these catalyze growth and enable rapid adaptation. When the crisis hit, one company moved all its full-time direct employees into a virtual operating environment; meanwhile, its outsourcing partner, the CEO recalled, “hid behind their contract and played one customer off against another.” It is not difficult to imagine who is better placed to succeed in the more flexible post-COVID-19 business environment, where value creation is shared and strategic partnerships matter even more. 4. Accelerate digital adoption to enable reimagination. Over the past few months, there has been a transformation in the way we interact with loved ones,  do our work, travel, get medical care, spend leisure time, and conduct many of the routine transactions of life. These changes have accelerated the migration to digital technologies at stunning scale and speed, across every sector. “We are witnessing what will surely be remembered as a historic deployment of remote work and digital access to services across every domain,” remarked one tech CEO.  He is right. Through the COVID-19 recovery, too, digital will play a defining role. During the early recovery period of partial reopening, business leaders will face some fundamental challenges. One is that consumer behavior and demand patterns have changed significantly and will continue to do so. Another is that how the economy lurches back to life will differ from country to country and even city to city. For example, consumers may feel comfortable going to restaurants before they will consider getting on a plane or going to sporting events. Early signals of increased consumer demand will likely come suddenly, and in clusters. Analyzing these demand signals in real time and adapting quickly to bring supply chains and services back will be essential for companies to successfully navigate the recovery. To address these challenges, leaders will need to set an ambitious digital agenda—and deliver it quickly, on the order of two to three months, as opposed to the previous norm of a year or more. There are four elements to this agenda: Refocus digital efforts to reflect changing customer expectations. To adapt, companies need to quickly rethink customer journeys and accelerate the development of digital solutions. The emphasis will be different for each sector. For many retailers, this includes creating a seamless e-commerce experience, enabling customers to complete everything they need to do online, from initial research and purchase to service and returns. For auto companies, this could mean establishing new digital distribution models to handle trade-ins, financing, servicing, and home delivery of cars. For industries such as airlines, ensuring health and safety will be essential, for example, by reinventing the passenger experience with “contactless” check- in, boarding, and in-flight experiences. Use data, Internet of Things, and AI to better manage operations. In parallel, companies need to incorporate new data and create new models to enable real-time decision making. In the same way that many risk and financial models had to be rebuilt after the 2008 financial crisis, the use of 7 From surviving to thriving: Reimagining the post-COVID-19 return data and analytics will need to be recalibrated to reflect the post-COVID-19 reality. This will involve rapidly validating models, creating new data sets, and enhancing modeling techniques. Getting this right will enable companies to successfully navigate demand forecasting, asset management, and coping with massive new volumes. For example, one airline developed a new app to manage and maintain its idle fleet and support bringing it back into service; and a North American telecommunications company developed a digital collection model for customers facing hardship. Accelerate tech modernization. Companies will also need to greatly improve their IT productivity to lower their cost base and fund rapid, flexible digital- solution development. First, this requires quickly reducing IT costs and making them variable wherever possible to match demand. This means figuring out what costs are flexible in the near-to-medium term, for example, by evaluating nonessential costs related to projects or maintenance, and reallocating resources. Second, this involves defining a future IT-product platform, establishing the skills and roles needed to sustain it, mapping these skills onto the new organization model, and developing leaders who can train people to fill the new or adapted roles. Third, the adoption of cloud and automation technologies will need to be speeded up, including bringing cloud operations on-premise and decommissioning legacy infrastructure. Increase the speed and productivity of digital solutions. To deal with the crisis and its aftermath, companies not only need to develop digital solutions quickly but also to adapt their organizations to new operating models and deliver these solutions to customers and employees at scale. Solving this “last mile” challenge requires integrating businesses processes, incorporating data-driven decision making, and implementing change management. There are different ways to do this. A wide variety of companies, from banks to mining operations, have accelerated delivery by establishing an internal “digital factory” with cross- functional teams dedicated to matching business priorities to digital practices. Others, in addition to reinventing their core businesses, have established new business–building entities to capture new opportunities quickly. For companies around the world, the qualities that brought Brazilian football to new heights in 1970— imagination, leadership, and on-the field execution— will be paramount as they consider how to navigate the post-COVID-19 environment. Business as usual will not be nearly enough: the game has changed too much. But by reimagining how they recover, operate, organize, and use technology, even as they return to work, companies can set the foundations for enduring success. Kevin Sneader, the global managing partner of McKinsey, is based in McKinsey’s Hong Kong office. Bob Sternfels is a senior partner in the San Francisco office. Designed by Global Editorial Services Copyright © 2020 McKinsey & Company. All rights reserved. 8 From surviving to thriving: Reimagining the post-COVID-19 return"]
# data= ["From surviving to thriving: Reimagining the post-COVID-19 return For many, the toughest leadership test is now looming"]
data = [re.sub('\S*@\S*\s?', '', sent) for sent in data]

data = [re.sub('\s+', ' ', sent) for sent in data]
data = [re.sub("\'", "", sent) for sent in data]
print(data)


def sent_to_words(sentences):
    for sentence in sentences:
        yield (gensim.utils.simple_preprocess(str(sentence), deacc=True))  # deacc=True removes punctuations


data_words = list(sent_to_words(data))
print(data_words)

# Build the bigram and trigram models
bigram = gensim.models.Phrases(data_words, min_count=2, threshold=50)  # higher threshold fewer phrases.
# trigram = gensim.models.Phrases(bigram[data_words], threshold=100)

# Faster way to get a sentence clubbed as a trigram/bigram
bigram_mod = gensim.models.phrases.Phraser(bigram)
# trigram_mod = gensim.models.phrases.Phraser(trigram)

# See trigram example
# print(trigram_mod[bigram_mod[data_words[0]]])


# Define functions for stopwords, bigrams, trigrams and lemmatization
def remove_stopwords(texts):
    return [[word for word in simple_preprocess(str(doc)) if word not in stop_words] for doc in texts]


def make_bigrams(texts):
    return [bigram_mod[doc] for doc in texts]


def make_trigrams(texts):
    return [trigram_mod[bigram_mod[doc]] for doc in texts]


def lemmatization(texts, allowed_postags=['NOUN', 'ADJ', 'VERB', 'ADV']):
    """https://spacy.io/api/annotation"""
    texts_out = []
    for sent in texts:
        doc = nlp(" ".join(sent))
        texts_out.append([token.lemma_ for token in doc if token.pos_ in allowed_postags])
    return texts_out


# Remove Stop Words
data_words_nostops = remove_stopwords(data_words)

# Form Bigrams
data_words_bigrams = make_bigrams(data_words_nostops)
print('+++++',data_words_bigrams)

# Initialize spacy 'en' model, keeping only tagger component (for efficiency)
# python3 -m spacy download en
# nlp =spacy.load('en_core_web_sm')
nlp = spacy.load('en_core_web_sm', disable=['parser', 'ner'])

# Do lemmatization keeping only noun, adj, vb, adv
data_lemmatized = lemmatization(data_words_bigrams, allowed_postags=['NOUN', 'ADJ', 'VERB', 'ADV'])
print("======",data_lemmatized)
# Create Dictionary
id2word = corpora.Dictionary(data_lemmatized)

# Create Corpus
texts = data_lemmatized
# print(texts)
# Term Document Frequency
corpus = [id2word.doc2bow(text) for text in texts]
# print(corpus)
# print([[(id2word[id], freq) for id, freq in cp] for cp in corpus[:]])

# lda_model = gensim.models.ldamodel.LdaModel(corpus=corpus,
#                                            id2word=id2word,
#                                            num_topics=20,
#                                            random_state=100,
#                                            update_every=1,
#                                            chunksize=100,
#                                            passes=10,
#                                            alpha='auto',
#                                            per_word_topics=True)
# # pprint(lda_model.print_topics())
# doc_lda = lda_model[corpus]
# print(lda_model.top_topics(corpus))
# print(lda_model.show_topics())
# with open('topic_file', 'w') as topic_file:
#     topics=lda_model.top_topics(corpus)
#     topic_file.write('\n'.join('%s %s' %topic for topic in topics))
# for i, row in enumerate(lda_model[corpus]):
#     print(i,row)
#     for j, (topic_num, prop_topic) in enumerate(row):
#         if j == 0:  # => dominant topic
#             wp = lda_model.show_topic(topic_num)
#             topic_keywords = ", ".join([word for word, prop in wp])
#             print(topic_keywords)
#     print(row)
# print(doc_lda)
# print(corpus)
# for j in doc_lda:
#     print(j)
# Compute Perplexity
# print('\nPerplexity: ', lda_model.log_perplexity(corpus))  # a measure of how good the model is. lower the better.

# # Compute Coherence Score
# coherence_model_lda = CoherenceModel(model=lda_model, texts=data_lemmatized, dictionary=id2word, coherence='c_v')
# import time
# time.sleep(5)
# coherence_lda = coherence_model_lda.get_coherence()
# print('\nCoherence Score: ', coherence_lda)
import os

os.environ['MALLET_HOME'] = 'C:\\Users\\vktiwari\\Downloads\\mallet-2.0.8'
mallet_path = 'C:\\Users\\vktiwari\\Downloads\\mallet-2.0.8\\bin\\mallet.bat'  # update this path


# ldamallet = gensim.models.wrappers.LdaMallet(mallet_path, corpus=corpus, num_topics=20, id2word=id2word)
#
# # Show Topics
# # pprint(ldamallet.show_topics(formatted=False))
#
# # Compute Coherence Score
# coherence_model_ldamallet = CoherenceModel(model=ldamallet, texts=data_lemmatized, dictionary=id2word, coherence='c_v')
# coherence_ldamallet = coherence_model_ldamallet.get_coherence()
#
# print('\nCoherence Score: ', coherence_ldamallet)
def compute_coherence_values(dictionary, corpus, texts, limit, start=2, step=3):
    """
    Compute c_v coherence for various number of topics

    Parameters:
    ----------
    dictionary : Gensim dictionary
    corpus : Gensim corpus
    texts : List of input texts
    limit : Max num of topics

    Returns:
    -------
    model_list : List of LDA topic models
    coherence_values : Coherence values corresponding to the LDA model with respective number of topics
    """
    coherence_values = []
    model_list = []
    for num_topics in range(start, limit, step):
        model = gensim.models.wrappers.LdaMallet(mallet_path, corpus=corpus, num_topics=num_topics, id2word=id2word)
        model_list.append(model)
        coherencemodel = CoherenceModel(model=model, texts=texts, dictionary=dictionary, coherence='c_v')
        coherence_values.append(coherencemodel.get_coherence())

    return model_list, coherence_values


def format_topics_sentences(ldamodels, corpus=corpus, texts=data):
    # Init output
    sent_topics_df = pd.DataFrame()

    for ldamodel in ldamodels:
        # Get main topic in each document
        for i, row in enumerate(ldamodel[corpus]):
            row = sorted(row, key=lambda x: (x[1]), reverse=True)
            # Get the Dominant topic, Perc Contribution and Keywords for each document
            for j, (topic_num, prop_topic) in enumerate(row):
                if j == 0:  # => dominant topic
                    wp = ldamodel.show_topic(topic_num)
                    topic_keywords = ", ".join([word for word, prop in wp])
                    sent_topics_df = sent_topics_df.append(
                        pd.Series([int(topic_num), round(prop_topic, 4), topic_keywords]), ignore_index=True)
                else:
                    break
    sent_topics_df.columns = ['Dominant_Topic', 'Perc_Contribution', 'Topic_Keywords']

    # Add original text to the end of the output
    contents = pd.Series(texts)
    sent_topics_df = pd.concat([sent_topics_df, contents], axis=1)
    return (sent_topics_df)


if __name__ == "__main__":
    model_list, coherence_values = compute_coherence_values(dictionary=id2word, corpus=corpus, texts=data_lemmatized,
                                                            start=2, limit=15, step=2)

    print(model_list,len(model_list))
    optimal_model = model_list
    print(optimal_model)
    # for i,mk in enumerate(optimal_model):
        # model_topics = optimal_model.show_topics(formatted=False)
        # pprint(optimal_model.print_topics(num_words=10))
    df_topic_sents_keywords = format_topics_sentences(optimal_model, corpus=corpus, texts=data)
    df_dominant_topic = df_topic_sents_keywords.reset_index()
    df_dominant_topic.columns = ['Document_No', 'Dominant_Topic', 'Topic_Perc_Contrib', 'Keywords', 'Text']

    # Format
    df_dominant_topic = df_topic_sents_keywords.reset_index()
    df_dominant_topic.columns = ['Document_No', 'Dominant_Topic', 'Topic_Perc_Contrib', 'Keywords', 'Text']
    print(df_dominant_topic.head(10))
    # filename= "topic_model_new_"+str(i)+".csv"
    filename = "topic_model_new_temp.csv"
    df_dominant_topic.to_csv(filename,sep = "\t")
