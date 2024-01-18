import subprocess
import os
import shutil
import argparse
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

# 脚本功能
# 用于将sd卡里面的照片按照日期和评分复制到指定文件夹中（按照年份和日期进行存储）
# 简化导入工作流 可以在相机中直接选片 设置评分 然后将sd卡插入电脑 运行脚本即可
# 评分大于等于指定值的照片会被复制到指定文件夹中
# 可以选择是否复制相应的.CR2文件
# 可以选择复制的日期范围
# 可以选择复制的最小评分值
# 可以在代码中调整线程池的大小（当前设置为8）

# 使用方法
# 不指定路径 这个就是将默认输入路径下 评分大于等于4分的照片以及相应的.CR2文件复制到默认输出路径下 建议在代码中将sd卡的路径和输出路径设置为默认值 这样不用每次都输入
# 然后命令行要输入一个开始日期和结束日期
# python3 photos.py --min_rating 4 --copy_cr2 --start_date 20210101 --end_date 20211231 
# 指定路径
# python3 photos.py --source_path /custom/source/path --dest_path /custom/destination/path --min_rating 4 --copy_cr2 --start_date 20210101 --end_date 20211231
# 设置命令行参数

parser = argparse.ArgumentParser(description='Filter and copy images based on EXIF data.')
parser.add_argument('--source_path', type=str, default='/Users/jeremy/pyspark/source', help='Source directory path')
parser.add_argument('--dest_path', type=str, default='/Users/jeremy/pyspark/dest', help='Destination directory path')
parser.add_argument('--min_rating', type=int, default=1, help='Minimum rating to filter images (default: 1)')
parser.add_argument('--copy_cr2', action='store_true', help='Copy corresponding .CR2 files along with JPEGs')
parser.add_argument('--start_date', type=str, default=None, help='Start date in YYYYMMDD format')
parser.add_argument('--end_date', type=str, default=None, help='End date in YYYYMMDD format')



def copy_corresponding_cr2(filename, source_directory, destination_subfolder):
    """Copy corresponding .CR2 file if exists."""
    base_filename = os.path.splitext(filename)[0]
    cr2_filename = base_filename + '.CR2'
    cr2_file_path = os.path.join(source_directory, cr2_filename)
    if os.path.isfile(cr2_file_path):
        shutil.copy2(cr2_file_path, destination_subfolder)
        print(f"Copied '{cr2_filename}' to the destination.")

def process_file(file_path, start_date, end_date, args, filename):
    # 这里放置处理单个文件的代码逻辑
    if os.path.isfile(file_path) and file_path.lower().endswith('.jpg'):
        try:
            # 使用exiftool获取评分信息 佳能对应的是Rating 如果其他牌子相机对应字段名不一样的话可以手动用exiftool工具查看 然后在代码这里硬编码就可以了
            result_rating = subprocess.run(['exiftool', '-s', '-s', '-s', '-Rating', file_path], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
            result_date = subprocess.run(['exiftool', '-s', '-s', '-s', '-DateTimeOriginal', file_path], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
            
            # 输出结果为字符串形式，包含了评分值和日期
            rating_str = result_rating.stdout.strip()
            date_str = result_date.stdout.strip()
            
            # 处理日期格式
            photo_date = datetime.strptime(date_str, '%Y:%m:%d %H:%M:%S') if date_str else None
            
            # 检查是否在日期范围内
            if photo_date and ((start_date is None or photo_date >= start_date) and (end_date is None or photo_date <= end_date)):
                # 如果有返回评分，且评分大于等于最小评分，则复制该文件
                if rating_str.isdigit() and int(rating_str) >= args.min_rating:
                    # 创建年份和日期子文件夹
                    year_folder = os.path.join(args.dest_path, str(photo_date.year))
                    date_folder = os.path.join(year_folder, photo_date.strftime('%Y%m%d'))
                    os.makedirs(date_folder, exist_ok=True)

                    # 将照片复制到对应的日期文件夹中
                    shutil.copy2(file_path, date_folder)
                    print(f"Copied '{filename}' to {date_folder}")

                    # 如果需要，复制相应的.CR2文件
                    if args.copy_cr2:
                        copy_corresponding_cr2(filename, args.source_path, date_folder)
        except Exception as e:
            print(f"Error processing file {file_path}: {e}")
    pass

def main():
    args = parser.parse_args()

    # 解析日期参数
    start_date = datetime.strptime(args.start_date, '%Y%m%d') if args.start_date else None
    end_date = datetime.strptime(args.end_date, '%Y%m%d') if args.end_date else None

    # 获取源路径下所有文件名列表
    all_files = [f for f in os.listdir(args.source_path) if f.lower().endswith('.jpg')]
    total_files = len(all_files)

    # 使用线程池处理文件，并设置进度条
    with ThreadPoolExecutor(max_workers=8) as executor:
        # 创建tqdm进度条对象
        futures = []
        for file in all_files:
            futures.append(executor.submit(process_file, os.path.join(args.source_path, file), start_date, end_date, args, file))
        for future in tqdm(as_completed(futures), total=total_files, unit="file"):
            try:
                # 获取返回结果（如果process_file函数有返回值）
                result = future.result()
            except Exception as e:
                print(f"Error processing file: {e}")

if __name__ == "__main__":
    main()


print("Done filtering images by rating.")
