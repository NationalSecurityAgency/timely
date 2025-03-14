package timely.collectd.plugin;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class SMARTCodeMapping extends HashMap<Integer,String> {

    private static final long serialVersionUID = 1L;

    private static final Map<Integer,String> smart;
    static {
        Map<Integer,String> map = new HashMap<>();
        /**
         * mapping from https://en.wikipedia.org/wiki/S.M.A.R.T.
         */
        map.put(1, "Read_Error_Rate");
        map.put(2, "Throughput_Performance");
        map.put(3, "Spin_Up_Time");
        map.put(4, "Start_Stop_Count");
        map.put(5, "Reallocated_Sectors_Count");
        map.put(6, "Read_Channel_Margin");
        map.put(7, "Seek_Error_Rate");
        map.put(8, "Seek_Time_Performance");
        map.put(9, "Power_On_Hours");
        map.put(10, "Spin_Retry_Count");
        map.put(11, "Calibration_Retry_Count");
        map.put(12, "Power_Cycle_Count");
        map.put(13, "Soft_Read_Error_Rate");
        map.put(22, "Current_Helium_Level");
        map.put(170, "Available_Reserved_Space");
        map.put(171, "SSD_Program_Fail_Count");
        map.put(172, "SSD_Erase_Fail_Count");
        map.put(173, "SSD_Wear_Leveling_Count");
        map.put(174, "Unexpected_power_loss_count");
        map.put(175, "Power_Loss_Protection_Failure");
        map.put(176, "Erase_Fail_Count");
        map.put(177, "Wear_Range_Delta");
        map.put(179, "Used_Reserved_Block_Count_Total");
        map.put(180, "Unused_Reserved_Block_Count_Total");
        map.put(181, "Program_Fail_Count_Total");
        map.put(182, "Erase_Fail_Count");
        map.put(183, "SATA_Downshift_Error_Count");
        map.put(184, "End_to_End_error");
        map.put(185, "Head_Stability");
        map.put(186, "Induced_Op_Vibration_Detection");
        map.put(187, "Reported_Uncorrectable_Errors");
        map.put(188, "Command_Timeout");
        map.put(189, "High_Fly_Writes");
        map.put(190, "Airflow_Temperature_Celsius");
        map.put(190, "Temperature_Difference_from_100");
        map.put(191, "G_sense_Error_Rate");
        map.put(192, "Unsafe_Shutdown_Count");
        map.put(193, "Load_Unload_Cycle_Count");
        map.put(194, "Temperature_Celsius");
        map.put(195, "Hardware_ECC_Recovered");
        map.put(196, "Reallocation_Event_Count");
        map.put(197, "Current_Pending_Sector_Count");
        map.put(198, "Uncorrectable_Sector_Count");
        map.put(199, "UltraDMA_CRC_Error_Count");
        // map.put(200,"Multi_Zone_Error_Rate_[41]");
        map.put(200, "Write_Error_Rate");
        map.put(201, "Soft_Read_Error_Rate");
        map.put(202, "Data_Address_Mark_errors");
        map.put(203, "Run_Out_Cancel");
        map.put(204, "Soft_ECC_Correction");
        map.put(205, "Thermal_Asperity_Rate");
        map.put(206, "Flying_Height");
        map.put(207, "Spin_High_Current");
        map.put(208, "Spin_Buzz");
        map.put(209, "Offline_Seek_Performance");
        map.put(210, "Vibration_During_Write");
        map.put(211, "Vibration_During_Write");
        map.put(212, "Shock_During_Write");
        map.put(220, "Disk_Shift");
        map.put(221, "G_Sense_Error_Rate");
        map.put(222, "Loaded_Hours");
        map.put(223, "Load_Unload_Retry_Count");
        map.put(224, "Load_Friction");
        map.put(225, "Load_Unload_Cycle_Count");
        map.put(226, "Load_In_time");
        map.put(227, "Torque_Amplification_Count");
        map.put(228, "Power_Off_Retract_Cycle");
        // map.put(230,"GMR_Head_Amplitude");
        map.put(230, "Drive_Life_Protection_Status");
        map.put(231, "Temperature");
        // map.put(231,"SSD_Life_Left");
        map.put(232, "Endurance_Remaining");
        // map.put(232,"Available_Reserved_Space");
        map.put(233, "Power_On_Hours");
        // map.put(233,"Media_Wearout_Indicator");
        map.put(234, "Average_erase_count");
        map.put(235, "Good_Block_Count");
        map.put(240, "Head_Flying_Hours");
        // map.put(240,"Transfer_Error_Rate_(Fujitsu)");
        map.put(241, "Total_LBAs_Written");
        map.put(242, "Total_LBAs_Read");
        map.put(243, "Total_LBAs_Written_Expanded");
        map.put(244, "Total_LBAs_Read_Expanded");
        map.put(249, "NAND_Writes_1GiB");
        map.put(250, "Read_Error_Retry_Rate");
        map.put(251, "Minimum_Spares_Remaining");
        map.put(252, "Newly_Added_Bad_Flash_Block");
        map.put(254, "Free_Fall_Protection");
        smart = Collections.unmodifiableMap(map);
    }

    protected SMARTCodeMapping() {}

    public int size() {
        return smart.size();
    }

    public boolean isEmpty() {
        return smart.isEmpty();
    }

    public boolean containsKey(Object key) {
        return smart.containsKey(key);
    }

    public boolean containsValue(Object value) {
        return smart.containsValue(value);
    }

    public String get(Object key) {
        return smart.get(key);
    }

    public String put(Integer key, String value) {
        return smart.put(key, value);
    }

    public String remove(Object key) {
        return smart.remove(key);
    }

    public void putAll(Map<? extends Integer,? extends String> m) {
        smart.putAll(m);
    }

    public void clear() {
        smart.clear();
    }

    public Set<Integer> keySet() {
        return smart.keySet();
    }

    public Collection<String> values() {
        return smart.values();
    }

    public Set<java.util.Map.Entry<Integer,String>> entrySet() {
        return smart.entrySet();
    }

    public boolean equals(Object o) {
        return smart.equals(o);
    }

    public int hashCode() {
        return smart.hashCode();
    }

}
