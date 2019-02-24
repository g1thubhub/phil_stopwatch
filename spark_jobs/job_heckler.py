from pyspark.sql import SparkSession
from pyspark import SparkContext
import datetime
import spacy
import pkgutil
import socket
import os

os.environ['PYSPARK_PYTHON'] = '/usr/local/bin/python3.6'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/usr/local/bin/python3.6'
os.environ["OBJC_DISABLE_INITIALIZE_FORK_SAFETY"] = "YES"
# OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES ~/spark-2.4.0-bin-hadoop2.7/bin/spark-submit


def check_spacy_setup(model_name):  # Should simulate CoreNLP's logging & packaging
    # Check for spacy itself:
    host_name = socket.gethostbyaddr(socket.gethostname())[0]
    installed_modules = set()
    for module_info in pkgutil.iter_modules():
        installed_modules.add(module_info.name)  # ModuleInfo(module_finder=FileFinder('/usr/local/lib/python3.6/site-packages'), name='spacy', ispkg=True)
    if 'spacy' not in installed_modules:
        print('^^ Warning: spacy might not have been installed on this host, ' + host_name)
    else:
        import spacy
        spacy_version = spacy.__version__
        print('^^ Using spaCy ' + spacy_version)
        data_path = spacy.util.get_data_path()  # spaCy data directory, e.g. /usr/local/lib/python3.6/site-packages/spacy/data
        full_model_path = os.path.join(data_path.as_posix(), model_name)
        if os.path.exists(full_model_path):
            print('^^ Model found at ' + full_model_path)
        else:
            print('^^ Model not found at ' + full_model_path + ', trying to download now')
            spacy.cli.download(model_name)


def fast_annotate_texts(iter, model_name):
    check_spacy_setup(model_name)
    nlp_model = spacy.load(model_name)
    print('^^ Created model ' + model_name)
    for element in iter:
        annotations = list()
        doc = nlp_model(element)
        for word in doc:
            annotations.append('//'.join((str(word), word.dep_)))
        yield ' '.join(annotations)


def slow_annotate_text(element, model_name):
    check_spacy_setup(model_name)
    nlp_model = spacy.load(model_name)
    print('^^ Created model ' + model_name)

    doc = nlp_model(element)
    annotations = list()
    for word in doc:
        annotations.append('//'.join((str(word), word.dep_)))
    return ' '.join(annotations)

if __name__ == "__main__":
    standard_model = 'en_core_web_sm'
    texts = list()
    for _ in range(0, 500):
        texts.append("The small red car turned very quickly around the corner.")
        texts.append("The quick brown fox jumps over the lazy dog.")
        texts.append("This is supposed to be a nonsensial sentence but in the context of this app it does make sense after all.")

    start = str(datetime.datetime.now())
    # Initialization:
    threads = 3  # program simulates a single executor with 3 cores (one local JVM with 3 threads)
    sparkContext = SparkContext('local[{}]'.format(threads), 'Profiling Heckler')
    session = SparkSession(sparkContext)

    parsed_strings1 = session.sparkContext.parallelize(texts) \
        .map(lambda record: slow_annotate_text(record, model_name=standard_model))

    parsed_strings2 = session.sparkContext.parallelize(texts) \
        .mapPartitions(lambda parition: fast_annotate_texts(parition, model_name=standard_model))

    # print(parsed_strings1.count())
    print(parsed_strings2.count())
