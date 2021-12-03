import xml.etree.ElementTree as et
import pandavro as pdx
import pandas as pd

def readfromxmltodict(path):
    # beolvassuk az XML fájlt
    tree = et.parse(path)
    root = tree.getroot()

    data = {}
    i = 0
    for child in root:
        data[i] = []
        for ch in child:
            if ch.tag == 'postalZip':
                zipcode_filter = filter(str.isdigit,ch.text)
                zipcode = ''.join(zipcode_filter)
                data[i].append(zipcode)
            elif ch.tag == 'name':
                first_name = ch.text.split(' ')[0]
                last_name = ch.text.split(' ')[1]
                data[i].append(first_name)
                data[i].append(last_name)
            else:
                data[i].append(ch.text)
        i+=1
    return data


dictData = readfromxmltodict("C:/Users/norbe/Documents/projektmunka3/generated_data.xml")

out_path="C:/Users/norbe/Documents/projektmunka3/modfied_data.avro"
pdx.to_avro(out_path, pd.DataFrame.from_dict(dictData[0]))
saved = pdx.read_avro(out_path)
print(saved)

print(pd.DataFrame.from_dict(dictData[1]))