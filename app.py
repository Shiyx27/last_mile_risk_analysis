import pandas as pd
import os
import io
from flask import Flask, request, render_template, send_file

app = Flask(__name__)

@app.route('/', methods=['GET', 'POST'])
def upload_file():
    if request.method == 'POST':
        file = request.files['file']
        if file:
            df = pd.read_csv(file)
            df['Order Creation Date'] = pd.to_datetime(df['Order Creation Date'], errors='coerce')
            df = df.sort_values(by=['Vehicle Number', 'Order Creation Date'], ascending=[True, True])

            df['Prev Manual End Odometer'] = df.groupby('Vehicle Number', group_keys=False).apply(
                lambda x: x.sort_values('Order Creation Date').shift(1))['Manual End Odometer (in meters)']

            def detect_risk(row):
                risks = set()
                reasons = set()
                risk_value = 0
                
                if pd.notna(row['Prev Manual End Odometer']) and pd.notna(row['Manual Start Odometer (in meters)']):
                    if row['Manual Start Odometer (in meters)'] < row['Prev Manual End Odometer']:
                        risks.add("Odometer inconsistency")
                        reasons.add("Odometer reading is less than the previous day's end reading")
                        risk_value += 20
                
                if row['GPS Available'] == 'Yes':
                    if pd.notna(row['Trip GPS Distance Travelled (in KM)']) and pd.notna(row['Manual Distance Travelled (in KM)']):
                        if abs(row['Manual Distance Travelled (in KM)'] - row['Trip GPS Distance Travelled (in KM)']) > 0.1:
                            risks.add("GPS discrepancy")
                            reasons.add("GPS distance and manual distance differ significantly")
                            risk_value += 10
                
                if pd.notna(row['Manual Distance Travelled (in KM)']) and row['Manual Distance Travelled (in KM)'] > 125:
                    risks.add("Excessive travel distance")
                    reasons.add("Manual distance travelled exceeds 125 KM in a day")
                    risk_value += 15
                
                return row['Order Creation Date'], row['Vehicle Number'], '; '.join(risks) if risks else None, '; '.join(reasons) if reasons else None, risk_value
            
            df[['Date', 'Vehicle Number', 'Risk Factors', 'Reasoning', 'Risk Value']] = df.apply(detect_risk, axis=1, result_type='expand')

            deviations_df = df[df['Risk Factors'].notna()][['Zone', 'Hub', 'Vehicle Number', 'Date', 'Risk Factors', 'Reasoning', 'Risk Value']]

            grouped_deviations = deviations_df.groupby(['Zone', 'Hub', 'Vehicle Number']).agg({
                'Date': lambda x: ', '.join(x.astype(str)),
                'Risk Factors': lambda x: '; '.join(set(x)),
                'Reasoning': lambda x: '; '.join(set(x)),
                'Risk Value': 'sum'
            }).reset_index()

            # Save in-memory instead of disk
            output = io.BytesIO()
            grouped_deviations.to_csv(output, index=False)
            output.seek(0)

            return render_template('index.html', top_20_hubs=grouped_deviations.to_dict(orient='records'),
                                   file_ready=True, file_data=output.getvalue())

    return render_template('index.html', top_20_hubs=[], file_ready=False, file_data=None)

@app.route('/download')
def download_file():
    output = io.BytesIO()
    output.write(request.args.get('file_data', '').encode())
    output.seek(0)
    return send_file(output, as_attachment=True, download_name="risk_analysis.csv", mimetype="text/csv")

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)