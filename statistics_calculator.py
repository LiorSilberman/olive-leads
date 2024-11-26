import pandas as pd
import asyncio

async def calculate_statistics(df):
    if df.empty:
        return "<p style='color: red; text-align: right;'>אין נתונים לחישוב סטטיסטיקה.</p>"

    await asyncio.sleep(1)
    # Enhanced CSS for better table readability with visible borders
    css = """
    <style>
        table {
            width: 100%;
            border-collapse: collapse;
            text-align: center;
            border: 1px solid black; /* Adds a border around the table */
            margin-bottom: 20px; /* Adds spacing after the table */
        }
        th, td {
            padding: 8px;
            border: 1px solid white; /* Adds visible borders for table cells */
            vertical-align: middle; /* Ensures text is centered vertically in cells */
            text-align: center;
        }
        tr:nth-child(even) {
            background-color: #f2f2f2; /* Alternating row colors for better readability */
        }
        tr:hover {
            background-color: #f5f5f5; /* Optional: highlights row on hover */
        }
        h2 {
            
            margin: 10px 0 10px 20px; /* Adds space above and below the heading */
        }
    </style>
    """

    # Source effectiveness calculation
    # Calculate source effectiveness and quantity
    source_effectiveness = df['מקור'].value_counts(normalize=True).sort_values(ascending=False) * 100
    source_quantity = df['מקור'].value_counts().sort_values(ascending=False)

    # Create a DataFrame for the sources
    source_effectiveness = pd.DataFrame({
        'מקור': source_effectiveness.index,
        'כמות': source_quantity.values,
        'אחוזים': source_effectiveness.values.round(2)
    })

    # Add a total row to the DataFrame
    total_row = pd.DataFrame({
        'מקור': ['סך הכל'],
        'כמות': [source_quantity.sum()],
        'אחוזים': [100]
    })
    source_effectiveness = pd.concat([source_effectiveness, total_row], ignore_index=True)

    # Generate HTML table with the DataFrame
    source_html = source_effectiveness.to_html(index=False, header=True, border=0, escape=False)


    # Subscription types calculation
    filtered_df = df[df['מנוי'].isin(['ללא', 'מנוי פריסייל']) == False]
    subscription_types = filtered_df['מנוי'].value_counts().reset_index(name='כמות')
    subscription_types.columns = ['מנוי', 'כמות']
    total_subscriptions = subscription_types['כמות'].sum()
    subscription_types['אחוז מסך כלל המנויים'] = (subscription_types['כמות'] / total_subscriptions) * 100
    subscription_types['אחוז מסך כלל המנויים'] = subscription_types['אחוז מסך כלל המנויים'].round(2)

    # Append a total row
    total_row = pd.DataFrame({
        'מנוי': ['סך הכל'],
        'כמות': [total_subscriptions],
        'אחוז מסך כלל המנויים': [round(subscription_types['אחוז מסך כלל המנויים'].sum())]
    })
    subscription_types = pd.concat([subscription_types, total_row], ignore_index=True)

    # Generate HTML table
    subscriptions_html = subscription_types.to_html(index=False, header=True, border=0)
    subscriptions_html = subscriptions_html.replace('<table>', "<table>")
    

    # Trial success rate calculation
    if df['עשו ניסיון'].eq('V').sum() > 0:
        did_trial = df['עשו ניסיון'].eq('V').sum()
        did_trial_and_members = (df[(df['עשו ניסיון'] == 'V') & df['מנוי'].notna() & (df['מנוי'] != 'ללא')].shape[0])
        trial_success_rate = (did_trial_and_members / did_trial) * 100
    else:
        trial_success_rate = 0
    

    # Trials by source calculation
    trial_data = df[df['עשו ניסיון'] == 'V']
    trial_by_source = trial_data.groupby('מקור').size().reset_index(name='מספר מתאמנות')

    subscription_count = (
        trial_data.groupby('מקור')['מנוי']
        .apply(lambda x: x.notna().sum() - (x == "ללא").sum())
        .reset_index(name='כמות מנויים')
    )
    trial_summary = pd.merge(trial_by_source, subscription_count, on='מקור', how='left')

    # Add percentage column
    trial_summary['אחוז מנויים'] = (trial_summary['כמות מנויים'] / trial_summary['מספר מתאמנות']) * 100

    # Round the percentage for better readability
    trial_summary['אחוז מנויים'] = trial_summary['אחוז מנויים'].round(2)

    # Add a total row to the DataFrame
    total_trials = trial_summary['מספר מתאמנות'].sum()
    total_subscriptions = trial_summary['כמות מנויים'].sum()

    total_row = pd.DataFrame({
        'מקור': ['סך הכל'],
        'מספר מתאמנות': [total_trials],
        'כמות מנויים': [total_subscriptions],
        'אחוז מנויים': [trial_success_rate.round(2)]
    })
    trial_summary = pd.concat([trial_summary, total_row], ignore_index=True)

    # Generate HTML table with the DataFrame
    trial_by_source_html = trial_summary.to_html(index=False, header=True, border=0, escape=False)
    

    # Calculate coaches count and subscription closures
    coaches_count = df['מאמנים'].value_counts().reset_index(name='כמות')
    coaches_count.columns = ['מאמנים', 'כמות']
    subscription_count = (
        df.groupby('מאמנים')['מנוי']
        .apply(lambda x: x.notna().sum())
        .reset_index(name='כמות מנויים שסגרו')
    )

    # Merge the two counts
    coaches_count = pd.merge(coaches_count, subscription_count, on='מאמנים', how='left')

    # Calculate closing percentage
    coaches_count['אחוזי סגירה'] = (coaches_count['כמות מנויים שסגרו'] / coaches_count['כמות']) * 100
    coaches_count['אחוזי סגירה'] = coaches_count['אחוזי סגירה'].round(2)

    # Add a total row
    total_coaches = coaches_count['כמות'].sum()
    total_subscriptions_closed = coaches_count['כמות מנויים שסגרו'].sum()
    total_closing_percentage = (total_subscriptions_closed / total_coaches * 100) if total_coaches > 0 else 0

    total_row = pd.DataFrame({
        'מאמנים': ['סך הכל'],
        'כמות': [total_coaches],
        'כמות מנויים שסגרו': [total_subscriptions_closed],
        'אחוזי סגירה': [total_closing_percentage.round(2)]
    })
    coaches_count = pd.concat([coaches_count, total_row], ignore_index=True)

    # Generate HTML table
    coaches_html = coaches_count.to_html(index=False, header=True, border=0, escape=False)


    # TODO - table with for each month from the col df['תאריך סיום'] and df['מנוי'] == 'ללא' count the number of leads for each month
    # df['תאריך סיום'] = pd.to_datetime(df['תאריך סיום'], errors='coerce')
    # filtered_df = df[df['מנוי'] == 'ללא']

    # monthly_leads = filtered_df.groupby(filtered_df['תאריך סיום'].dt.to_period('M')).size().reset_index(name='כמות מנויים שנטשו')
    # monthly_leads.columns = ['חודש', 'כמות מנויים שנטשו']
    # monthly_leads_html = monthly_leads.to_html(index=False, border=0)
    
    # TODO - table for each 'מקור' how much quantity come from and from each quantity, how much 'יש מנוי' == 'V' and add total row
    
    source_with_subscription = df[(df['יש מנוי'] == 'V') & (df['מנוי'] != 'מנוי פריסייל')]['מקור'].value_counts().reindex(source_quantity.index, fill_value=0)
    source_quantity = df['מקור'].value_counts().sort_values(ascending=False)

    source_summary = pd.DataFrame({
        'כמות': source_quantity,
        'כמות עם מנוי': source_with_subscription,
        'אחוז מנויים': (source_with_subscription / source_quantity * 100).round(2)
    })

    total_row = pd.DataFrame({
        'כמות': [source_quantity.sum()],
        'כמות עם מנוי': [source_with_subscription.sum()],
        'אחוז מנויים': [(source_with_subscription.sum() / source_quantity.sum() * 100).round(2)]
    }, index=['סך הכל'])

    source_summary = pd.concat([source_summary, total_row])
    source_summary.reset_index(inplace=True)
    source_summary.rename(columns={'index': 'מקור'}, inplace=True)
    source_summary_html = source_summary.to_html(index=False, header=True, border=0, escape=False)


    age_distribution = df['גיל'].mean()

    # Combine all HTML parts with the CSS header
    stats = f"{css}<div><h2>אחוזי קליטה עבור כל מקור: </h2>{source_html}</div>" \
            f"<div><h2>מספר לידים עבור כל מקור וסגירת מנויים עבור כל מקור: </h2>{source_summary_html}</div>" \
            f"<div><h2>מספר אימוני ניסיון שהגיעו עבור כל מקור: </h2>{trial_by_source_html}</div>" \
            f"<div><h2>סוגי מנויים:</h2>{subscriptions_html}</div>" \
            f"<div><h2>מאמנות:</h2>{coaches_html}</div>" \
            f"<div><h2>הצלחת שיעורי המרה:</h2> <ul><li><h3>מספר המתאמנים שעשו אימון ניסיון: {did_trial}</h3></li><li><h3>מספר מנויים שעשו אימון ניסיון: {did_trial_and_members}</h3></li> <li><h3>הצלחת שיעורי המרה באחוזים: {trial_success_rate:.2f}%</h3></li></ul></div>" \
            f"<div><h2>ממוצע גילאים: {age_distribution:.2f}</h2></div>"
    return stats