import os

def warc_all():
    try:
        path='/home/alejo/Documentos/rpse/input'
        line = ''
        for fil in os.listdir(path):
            if(not fil in 'warc_prueba.txt'):
                f=open(path+'/'+fil, 'r')
                line=line+'\n'+f.read()
            else:
                print(fil)
            
        o=open(path+"/warc_all.txt", 'a')
        o.write(line)
        o.close()
    except Exception as err:
        print("As an error detail: "+str(err))
if __name__ == '__main__':
    warc_all()