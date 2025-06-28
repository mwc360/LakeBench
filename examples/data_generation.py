from lakebench.datagen.tpcds import TPCDSDataGenerator

datagen = TPCDSDataGenerator(
    scale_factor=1,
    target_mount_folder_path='/lakehouse/default/Files/tpcds_sf1'
)
datagen.run()