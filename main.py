from lector_reglas_repo import LectorReglasRepo


def main():
    lector = LectorReglasRepo()

    # datos_componentes = lector.componentes("C:/Users/jortegoi/OneDrive - NTT DATA EMEAL/Escritorio/Test/dqkcmcalpha")
    # print(datos_componentes)

    # datos_reglas = lector.reglas("C:/Users/jortegoi/OneDrive - NTT DATA EMEAL/Escritorio/Test/dqkcmcalpha")
    # print(datos_reglas)

    datos_componentes = lector.componentes("C:/Users/jortegoi/OneDrive - NTT DATA EMEAL/Escritorio/Test/glingestaskbtqalpha")
    print(datos_componentes)
    # datos_componentes.to_csv("componentes.csv")

    datos_reglas = lector.reglas("C:/Users/jortegoi/OneDrive - NTT DATA EMEAL/Escritorio/Test/glingestaskbtqalpha")
    print(datos_reglas)
    # datos_reglas.to_csv("reglas.csv")
    


if __name__ == "__main__":
    main()