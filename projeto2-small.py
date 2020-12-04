import pyspark
import math
import pandas as pd

def analise(palavras_recebidas, rdd, rdd_idf):

    def conta_palavras(item):
        url, conteudo = item
        palavras = conteudo.strip().split()
        return [(palavra.lower(), 1) for palavra in palavras]

    def computa_freq_palavra(item):
        palavra, contagem = item
        palavra_freq = math.log10(1 + contagem)
        return (palavra, palavra_freq)

    def computa_relevancia(item):
        palavra, tup = item
        frequencia, idf = tup
        return (palavra, frequencia * idf)     

    def acha_palavra1_distinct(item):
        url, texto = item

        texto = texto.strip().split()
        texto_limpo = [x.lower() for x in texto]

        if palavras_recebidas[1] in texto_limpo:
            return False
        if palavras_recebidas[0] in texto_limpo:
            return True
        else: 
            return False  

    def acha_palavra(item):
        url, texto = item
        texto =  texto.strip().split()
        texto_limpo = [x.lower() for x in texto]
        if palavras_recebidas[0] not in texto_limpo or palavras_recebidas[1] not in texto_limpo:
            return False
        else:    
            return True  

    def corta_documento(item):
        url, texto = item
        texto =  texto.strip().split()
        texto_limpo = [x.lower() for x in texto]
        palavras_boas = []
        for i in range(5, len(texto_limpo)-5):
            if palavras_recebidas[0] in texto_limpo[i-5:i+5] or palavras_recebidas[1] in texto_limpo[i-5:i+5]:
                palavras_boas.append(texto_limpo[i])
        return (url, " ".join(palavras_boas))
            

    
    if palavras_recebidas[2] == 1:
        rdd = rdd.filter(acha_palavra)
        print("Entrou no juntas")
    else:
        rdd = rdd.filter(acha_palavra1_distinct)
        print("Entrou no separado")
    
    
    rdd_palavras = rdd.map(corta_documento).flatMap(conta_palavras).reduceByKey(junta_contagens).cache()
    rdd_palavras_freq = rdd_palavras.map(computa_freq_palavra)
    
    rdd_join = rdd_palavras_freq.join(rdd_idf)
    rdd_relevante = rdd_join.map(computa_relevancia)
    top_100 = rdd_relevante.takeOrdered(100, lambda x : -x[1])

    return top_100

    
if __name__ ==  '__main__':
    
    palavras_recebidas = ["apple", "samsung", 1]
     
    sc = pyspark.SparkContext(appName="Projeto2")
    rdd = sc.sequenceFile("s3://megadados-alunos/web-brasil") 
    #rdd = sc.sequenceFile("part-00000") 
    N = rdd.count()
    DOC_COUNT_MAX = 0.8*N
    DOC_COUNT_MIN = 5

    def conta_palavras_doc(item):
        url, conteudo = item
        palavras = conteudo.strip().split()
        return [(palavra.lower(), 1) for palavra in set(palavras)]

    def junta_contagens(nova_contagem, contagem_atual):
        return nova_contagem + contagem_atual

    def filtra_doc_freq(item):
        palavra, contagem = item
        return (contagem < DOC_COUNT_MAX) and (contagem >= DOC_COUNT_MIN) and (palavra.isalpha())

    def computa_idf(item):
        palavra, contagem = item 
        idf = math.log10(N/contagem)
        return (palavra, idf)
    

    rdd_doc_freq = rdd.flatMap(conta_palavras_doc).reduceByKey(junta_contagens).cache()
    rdd_doc_freq_filtrado = rdd_doc_freq.filter(filtra_doc_freq)
    rdd_idf = rdd_doc_freq_filtrado.map(computa_idf)    

    print("Começou a analise juntas")
    top_100_juntas = analise(palavras_recebidas, rdd, rdd_idf)
    df = pd.DataFrame(top_100_juntas, columns=["Palavra", "Relevância"])
    df.to_csv("s3://megadados-alunos/giovanna-mayra/juntas.csv")  
    #df.to_csv(name) 
    print("Terminou a analise juntas")
    palavras_recebidas1 = ["apple", "samsung", 0]
    print("Começou a analise apple")
    top_100_apple = analise(palavras_recebidas1, rdd, rdd_idf)
    df = pd.DataFrame(top_100_apple, columns=["Palavra", "Relevância"])
    df.to_csv("s3://megadados-alunos/giovanna-mayra/apple.csv")
    print("Terminou a analise apple")
    palavras_recebidas2 = ["samsung", "apple", 0]
    print("Começou a analise samsung")
    top_100_samsung = analise(palavras_recebidas2, rdd, rdd_idf)
    df = pd.DataFrame(top_100_samsung, columns=["Palavra", "Relevância"])
    df.to_csv("s3://megadados-alunos/giovanna-mayra/samsung.csv")
    print("Terminou a analise samsung")
    sc.stop() 
    