import os
import string
from PIL import Image, ImageDraw, ImageFont
import random
from datetime import datetime, timedelta
from faker import Faker
import pandas as pd
import csv
import shutil


script_dir = os.path.dirname(os.path.abspath(__file__))
os.chdir(script_dir)


def generate_random_data():
    """Generate random name, number, and dates"""
    fake = Faker()

    random_proportion = 0.92  # Percentage of perfectly fine created data. The remaining percentage will be the data with failures.

    # NAME
    if random.random() < random_proportion:

        name_parts = []
        for i in range(4):
            if i < 2:
                name_parts.append(fake.first_name())
            else:
                name_parts.append(fake.last_name())


        full_name = ' '.join(name_parts)

    else:
        full_name = ''

    # NUMBER, (MRN)
    if random.random() < random_proportion:
        nine_digit_number = f"{random.randint(0, 999999999):09d}"  # Generate 9-digit number with leading zeros
    else:
        nine_digit_number = ''.join(random.choice(string.punctuation) for _ in range(10))

    # NUMBER 2, (acc_num)
    if random.random() < random_proportion:
        acc_num = f"{random.randint(0, 999999999):09d}"  # Generate 9-digit number with leading zeros
    else:
        acc_num = ''.join(random.choice(string.punctuation) for _ in range(10))

    # DATES, (admit_date, discharge_date, dob)
    if random.random() < random_proportion:

        date_format = '%m/%d/%Y'
        start_date = datetime(2000, 1, 1)
        end_date = datetime(2025, 1, 1)

        fake_date_from = fake.date_between(start_date=start_date, end_date=end_date - timedelta(days=1))  # Random date_from between 2000-01-01 and 2024-12-31
        fake_date_until = fake.date_between(start_date=fake_date_from, end_date=end_date + timedelta(days=1))  # Random date_until between date_from and 2025-01-02
        fake_dob = fake.date_between(start_date=fake_date_from - timedelta(days=25200), end_date=fake_date_from)  # Random date_from between 2000-01-01 and 2024-12-31

        date_from = fake_date_from.strftime(date_format)
        date_until = fake_date_until.strftime(date_format)
        dob = fake_dob.strftime(date_format)

    else:
        date_from = ''
        date_until = ''
        dob = ''

    return {
        'name': full_name,
        'number': nine_digit_number,
        'acc_num': acc_num,
        'dob': dob,
        'date_from': date_from,
        'date_until': date_until
    }


def create_image(data, filename):
    """Create an image with the provided data"""
    # Create a blank image with white background
    width, height = 440, 260
    image = Image.new('RGB', (width, height), 'white')
    draw = ImageDraw.Draw(image)
    font = ImageFont.load_default(size=20)

    # Calculate positions
    y_position = 20
    line_height = 40

    # Draw each line of data
    draw.text((50, y_position), 'Name: ' + data['name'], fill='black', font=font)
    draw.text((50, y_position + line_height), 'MRN: ' + data['number'], fill='black', font=font)
    draw.text((50, y_position + 2 * line_height), 'acc_num: ' + data['acc_num'], fill='black', font=font)
    draw.text((50, y_position + 3 * line_height), 'DOB: ' + data['dob'], fill='black', font=font)
    draw.text((50, y_position + 4 * line_height), 'Admit date: ' + data['date_from'], fill='black', font=font)
    draw.text((50, y_position + 5 * line_height), 'Discharge date: ' + data['date_until'], fill='black', font=font)

    image.save(filename, 'JPEG')  # Save the image


def main():

    for _ in range(0, 5):  # Amount of batches we want to create
        batch_name = f"elegante_sample_{random.randint(0, 9999):06d}"  # Generate 6-digit number with leading zeros
        images_inner_path = 'Images/00001'
        images_path = f'{batch_name}/{images_inner_path}'
        os.makedirs(images_path, exist_ok=True)  # Create directory if it doesn't exist

        files = []
        partial_file_paths = []

        for i in range(0, 20):  # Generate 20 Images

            data = generate_random_data()

            filename = f'file-{i:03d}.jpg'
            file_path = f'{images_path}/{filename}'

            files.append(filename)
            partial_file_paths.append(f'{images_inner_path}/{filename}')

            create_image(data, file_path)
            print(f'Created: {file_path}')


        df = pd.DataFrame({
        'Image Filename': files,
        'Image Path': partial_file_paths
        })

        df['Box Barcode'] = '534509'
        df['Ref1 (Auto Extracted)'] = ''
        df['Ref2 (Auto Extracted)'] = ''
        df['Ref3 (Auto Extracted)'] = ''
        df['Ref4 (Auto Extracted)'] = ''
        df['Ref5 (Auto Extracted)'] = ''
        df['Ref6 (Auto Extracted)'] = ''
        df['Ref10 (Auto Extracted)'] = ''
        df['Project'] = 'Alegent Health'
        df['Collection'] = 'NK000145'
        df['Session'] = 'scott.peery-F21_202506061043'
        df['Date Created'] = '6/6/2025 10:51'
        df['Image Notes'] = ''
        df['Box Notes'] = ''
        df['Session Notes'] = 'T903945'
        df['Ref7 (Auto Extracted)'] = ''
        df['Ref8 (Auto Extracted)'] = ''
        df['Ref9 (Auto Extracted)'] = ''
        df['Ref11 (Auto Extracted)'] = ''

        print(df)

        df.to_csv(f'{batch_name}/loadfile.csv', sep='|', quoting=csv.QUOTE_ALL, index=False)

        shutil.make_archive(f"{batch_name}", 'zip', f'{batch_name}')

if __name__ == "__main__":
    main()