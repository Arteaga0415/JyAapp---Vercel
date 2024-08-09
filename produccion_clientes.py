import pandas as pd
import sys  

class ProduccionClientes:
  def __init__(self, file_path):
    self.file_path = file_path
    self.cleaned_df = None
    self.grouped_df = None
    self.df_with_index = None

  def load_and_clean_data(self):
    df = pd.read_excel(self.file_path)
    self.cleaned_df = df.dropna(subset=['Aseguradora', 'SumaDePrima_Participacion', 'SumaDePrima_Participacion'])
  
  def group_data(self):
     # Group by 'Ramo' and calculate the sum for 'Suma de Prima', 'Suma de Comision', and each client
    self.grouped_df = self.cleaned_df.groupby('Tomador').agg(
    Primas_Totales=pd.NamedAgg(column='SumaDePrima_Participacion', aggfunc='sum'),
    Comisiones=pd.NamedAgg(column='SumaDeValor_Comi_Gene_Recibos', aggfunc='sum'),
  ).reset_index()
    
  def reset_index(self):
    self.df_with_index = self.grouped_df.reset_index(drop=True)

  def drop_clients(self):
    clients = [
    'BERNAL R SAS', 'BOSQUE OIL EXPRESS SHOP S.A.S.', 'C.I PROMARCAS S.A.S.', 'C.I.R INGENIERIA S.A.S.', 'CARPAS Y LUBRICANTES LUFER S.A.S.', 
    'CASINO SERVICES S.A.S.', 'COLORETTO', 'COMERCIALIZADORA NACIONAL DE ALIMENTOS', 'COMERCIALIZADORA S Y E Y CIA S A.', 
    'CONDUGAS S.A.', 'CORPORACION RURAL LABORATORIO DEL', 'DISEÑOS EXCLUSIVOS S.A.S', 'DR. UNIFORM S.A.S', 'ECODIGITALY S.A.S',
    'ECOMARKET S.A.S.', 'EDIFICIO OTRABANDA', 'FUNDACION CLUB CAMPESTRE', 'FUNDACION DIEGO ECHAVARRIA MISAS CENTRO', 'INDUSTRIAS ALIMENTICIAS PERMAN S.A',
    'INMOBILIARIA GOMEZ Y ASOCIADOS', 'O&I INSPECCIONES S.A.S', 'P.C. CHAMPION S.A.S', 'PABILOPEZ S.A.S', 'SOLUCIONES E INNOVACIO EN ALIMENTOS', 
    'C.I. TEEN CLUB S.A.', 'TRES M-A.S.A.S.', 'UNION MEDICAL S.A.S.', 'VEMESA S.A.S', 'VISUAL SYSTEMS S.A.', 'UNION TEMPORAL CASTILLA SYEMA',
    ]
    i = 0;
    for client in self.grouped_df['Tomador']:
      if 'S.A.S' not in client and client not in clients:
        self.df_with_index.drop(index=i, inplace=True)
      i += 1;

  def rename_and_save(self, output_file_path='Producciones_por_Clientes.xlsx'):
    self.df_with_index.rename(columns={
      'Tomador': 'Cliente',
      'SumaDePrima_Participacion': 'Produccion',
      'SumaDeValor_Comi_Gene_Recibos': 'Comisiones'
    }, inplace=True)

    self.df_with_index.to_excel(output_file_path, index=False)
    print(f"Data has been successfully processed and saved to {output_file_path}")

  def process_data(self):
    self.load_and_clean_data()
    self.group_data()
    self.reset_index()
    self.drop_clients()
    self.rename_and_save()

if __name__ == "__main__":
  if len(sys.argv) != 2:
    print("Usage: python produccion_clientes.py <file_path>")
    sys.exit(1)

  file_path = sys.argv[1]
  produccion_clientes = ProduccionClientes(file_path)
  produccion_clientes.process_data()