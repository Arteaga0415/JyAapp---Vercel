import pandas as pd
import sys  

class ProduccionRamos:
    def __init__(self, file_path):
        self.file_path = file_path
        self.cleaned_df = None
        self.grouped_df = None
    
    def load_and_clean_data(self):
        df = pd.read_excel(self.file_path)
        self.cleaned_df = df.dropna(subset=['Aseguradora'])
    
    def add_insurance_columns(self):
        insurance_companies = [
            'SEGUROS SURAMERICANA S.A', 'SEGUROS BOLIVAR', 'HDI SEGUROS S.A', 'COMPAÑÍA MUNDIAL DE SEGUROS S A', 'Confianza', 
            'SBS SEGUROS COLOMBIA S.A.', 'SEGUROS DEL ESTADO S A', 'ASEGURADORA SOLIDARIA DE COLOMBIA', 'ALLIANZ SEGUROS S.A.', 
            'CHUBB DE COLOMBIA COMPAÑÍA SEGUROS S A', 'COMPAÑÍA ASEGURADORA DE FIANZAS S A'
        ]
        
        self.cleaned_df['Otras'] = self.cleaned_df.apply(lambda row: row['SumaDePrima_Participacion'] if row['Aseguradora'] not in insurance_companies else 0, axis=1)
        
        for company in insurance_companies:
            self.cleaned_df[company] = self.cleaned_df.apply(lambda row: row['SumaDePrima_Participacion'] if row['Aseguradora'] == company else 0, axis=1)
    
    def group_data(self):
        self.grouped_df = self.cleaned_df.groupby('Ramo').agg(
            Primas_Totales=pd.NamedAgg(column='SumaDePrima_Participacion', aggfunc='sum'),
            Comisiones=pd.NamedAgg(column='SumaDeValor_Comi_Gene_Recibos', aggfunc='sum'),
            Sura=pd.NamedAgg(column='SEGUROS SURAMERICANA S.A', aggfunc='sum'),
            Bolivar=pd.NamedAgg(column='SEGUROS BOLIVAR', aggfunc='sum'),
            HDI=pd.NamedAgg(column='HDI SEGUROS S.A', aggfunc='sum'),
            Mundial=pd.NamedAgg(column='COMPAÑÍA MUNDIAL DE SEGUROS S A', aggfunc='sum'),
            Confianza=pd.NamedAgg(column='Confianza', aggfunc='sum'),
            SBS=pd.NamedAgg(column='SBS SEGUROS COLOMBIA S.A.', aggfunc='sum'),
            S_del_Estado=pd.NamedAgg(column='SEGUROS DEL ESTADO S A', aggfunc='sum'),
            Solidaria=pd.NamedAgg(column='ASEGURADORA SOLIDARIA DE COLOMBIA', aggfunc='sum'),
            Allianz=pd.NamedAgg(column='ALLIANZ SEGUROS S.A.', aggfunc='sum'),
            Chubb=pd.NamedAgg(column='CHUBB DE COLOMBIA COMPAÑÍA SEGUROS S A', aggfunc='sum'),
            Finanzas=pd.NamedAgg(column='COMPAÑÍA ASEGURADORA DE FIANZAS S A', aggfunc='sum'),
            Otras=pd.NamedAgg(column='Otras', aggfunc='sum')
        ).reset_index()
    
    def rename_and_save(self, output_file_path='Producciones_por_Ramos_y_Compañias.xlsx'):
        self.grouped_df.rename(columns={
            'Ramo': 'Ramo (Tipo)',
            'SumaDePrima_Participacion': 'Primas Totales',
            'SumaDeValor_Comi_Gene_Recibos': 'Comisiones'
        }, inplace=True)
        
        self.grouped_df.to_excel(output_file_path, index=False)
        print(f"Data has been successfully processed and saved to {output_file_path}")
    
    def process_data(self):
        self.load_and_clean_data()
        self.add_insurance_columns()
        self.group_data()
        self.rename_and_save()

# Example usage:
# file_path = 'BaseDatosJyA.xlsx'
# produccion_ramos = ProduccionRamos(file_path)
# produccion_ramos.process_data()

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python ProduccionRamos.py <file_path>")
        sys.exit(1)
    
    file_path = sys.argv[1]
    produccion_ramos = ProduccionRamos(file_path)
    produccion_ramos.process_data()