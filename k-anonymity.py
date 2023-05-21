import collections
import pandas as pd
from datetime import datetime

###################################################
########           Helper Methods         #########
###################################################
'''
Mergig several dataframes that have been extracted in different time windows

'''
def merge_dfs(df_list):
    final_df = df_list[0]
    print(final_df)
    cntr = 0
    for df in df_list[1:]:
        final_df = final_df.append(df)
    return final_df
'''
time window method to use in the the detection K-Anonymity algorithm at the first point of check in and check out counter
returns:
        the trips that happend in a time window, like all trips between (5:00, 6:00)
'''
def get_time_frames(time_window = 120):
    timestamps = []
    if time_window <= 60:
        end_time = 2300 + time_window
        for i in range(500, end_time, 100):
            cnt = int(60/time_window)
            for j in range(cnt):
                temp_time = i + j*time_window
                if temp_time <= 2400:
                    timestamps.append(temp_time)

    if time_window > 60:
        base_time_window = 60
        end_time = 2300 + base_time_window
        for i in range(500, end_time, 100):
            cnt = int(60/base_time_window)
            for j in range(cnt):
                temp_time = i + j*base_time_window
                if temp_time <= 2400:
                    timestamps.append(temp_time)
        step = int(time_window / 60)
        timestamps = timestamps[::step]
    timeframes = [(timestamps[i], timestamps[i+1]) for i in range(len(timestamps)) if i!=len(timestamps)-1]
    return timeframes
###################################################
######## Extracting average of time trips #########
###################################################
'''
calculating mean and std for the average time between to distance
returns:
        a dictionary of the name of line and the value of the interval[t-s, t+s] for each <Lin, Lout>
'''
def get_lines_distance(df):
    in_out = []
    distance = []
    avereage_dict = {}
    for index, row in df.iterrows():
        in_out.append("{}{}".format(row["in_p_gis"], row["out_p_gis"]))
        distance.append(int((row["check_out"] - row["check_in"]).seconds /60))
    df["in_out"] = in_out
    df["distance"] = distance
    distances_df = df.groupby('in_out', as_index=False)['distance'].mean()
    distances_df_std = df.groupby('in_out', as_index=False)['distance'].std()
    print(" std is loading . . . ")
    time_frames = []
    for dist_mean, dist_std in zip(list(distances_df["distance"]), list(distances_df_std["distance"])):
        if dist_std < 1:
            time_frames.append((dist_mean-1, dist_mean+1))
        else:
            time_frames.append((dist_mean-dist_std, dist_mean+dist_std))
    ###### the dictionary which used in mating part to check arrival time
    distances_dict = dict(zip(list(distances_df["in_out"]), time_frames))
    print(" distances_dict is loading . . . ")
    return distances_dict
##############################################################
######### Applying detection K-Anonymity on counters #########
##############################################################
'''
applying correction step of detection K-Anonymity

'''
def apply_correction_stage(line_data,  new_col_name, col_name="binary_ids", k=1):
    unique_line_data = line_data.groupby(col_name).filter(lambda x: len(x) < k)
    duplicates_line_data = line_data.groupby(col_name).filter(lambda x: len(x) >= k)
    unique_line_data = unique_line_data.sort_values(by=['binary_ids'], ascending=True)
    print(unique_line_data['binary_ids'].to_string())
    duplicates_line_data = duplicates_line_data.sort_values(by=['binary_ids'], ascending=True)
    has_unique = 0
    if len(unique_line_data)>0:
        has_unique = 1
    if has_unique==1:
        print("\n - - - - \n unique_line_data: \n{}\n\n".format(unique_line_data))
        print("////////////////////////////////////////////////////")
        print(len(unique_line_data))
    ######## assining same ids to the unique ids
    final_df = pd.DataFrame()
    if len(unique_line_data) >=k :
        mod = len(unique_line_data) % k
        first_part_size = int(len(unique_line_data)/k)
        first_part_lst = list(unique_line_data[col_name])[:first_part_size]
        total_lst = first_part_lst * k
        total_lst.extend(total_lst[:mod])
        unique_line_data[new_col_name] = total_lst
        final_df = final_df.append(unique_line_data)
    ########## adding duplicate ids to the "binary_ids" column without any change
    if len(duplicates_line_data) > 0:
        duplicates_line_data[new_col_name] = duplicates_line_data[col_name]
        final_df = final_df.append(duplicates_line_data)
    return final_df, has_unique
##############################################################
#########      apply K-Anonymity based on time       #########
##############################################################
'''
for each timeframe like (5:00,6:00) it check all stations check in/out to
assign correction at that time and then save on the server to preserve privacy,
 we separate data to two different table of source and target

returns:
        a dataframe of the Beijing data with preserving the privacy that is safe to save on the server
'''
def divide_with_time_window(df, time_col_name="check_in", station_name_col="in_p_gis", new_col_name="id_in", col_name_correction="binary_ids", time_window=60):
    temp_timeframes = get_time_frames(time_window)
    timeframes = []
    for tf in temp_timeframes:
        tf0 = datetime.strptime(str(tf[0]),"%H%M")
        tf1 = datetime.strptime(str(tf[1]),"%H%M")
        timeframes.append((tf0, tf1))
    output_dfs = []  ##### contains several dataframes of time windows
    for time_frame in timeframes:
        stations = set(list(df[station_name_col]))
        for station in stations:
            output_df = pd.DataFrame()
            for index,row in df.iterrows():
                ##### finding all trips for the every single station in a specific time frame to assign correction
                if row[time_col_name] >= time_frame[0] and row[time_col_name] < time_frame[1] and row[station_name_col] == station:
                    output_df = output_df.append(row)
            if len(output_df)>1:
                output_dfs.append(output_df)
############### calling K-Anonymity method to assign it
    corrected_dfs = []
    has_unique_cntr = 0
    if len(output_dfs) > 0:
        for out_df in output_dfs:
            cdf,has_unique = apply_correction_stage(out_df, new_col_name)
            corrected_dfs.append(cdf)
            if has_unique == 1:
                has_unique_cntr += 1

    return corrected_dfs
##############################################################
#########    Match lines based on check in/out       #########
##############################################################
'''
 Remember we had two tables of check-in and check out with a privacy preserving which now we want to match them by this algorithm

returns:
        A final table of trips which it matched the lines based on comparing Ids and average time
'''
def match_lines(check_in_df, check_out_df, lines_distance_dict):

    output_df = pd.DataFrame(columns=["binary_ids", "in_p_gis", "out_p_gis", "check_in", "check_out"])
    ln_c_out_df = len(check_out_df)
    for index_out, row_out in check_out_df.iterrows():
        print("\r {}/{} calculated | part 1 ".format(index_out+1, ln_c_out_df), end="")
        for index_in, row_in in check_in_df.iterrows():

            if row_out['id_out'] == row_in['id_in']:
                line_distance_range = lines_distance_dict.get("{}{}".format(row_in['in_p_gis'], row_out['out_p_gis']))
                line_distance = row_out['check_out'] - row_in['check_in']
                if line_distance_range != None and (line_distance >= line_distance_range[0] and line_distance <= line_distance_range[1]):
                    check_in_df = check_in_df.drop([index_in], axis=0)
                    check_out_df = check_out_df.drop([index_out], axis=0)
                    output_df = output_df.append({"binary_ids": row_out['id_out'], "in_p_gis": row_in['in_p_gis'], "out_p_gis": row_out['out_p_gis'], "check_in": row_in['check_in'], "check_out": row_out['check_out']}, ignore_index=True)
                    break
    return output_df
##############################################################
###   Count trips based on time and station filters      #####
##############################################################
def trip_counter(df, line_in=None, line_out=None, start_time=500, end_time=2400):
    start_time = datetime.strptime(str(start_time),"%H%M")
    end_time = datetime.strptime(str(end_time),"%H%M")
    cntr = 0
    output_df = pd.DataFrame()

    if line_in == None and line_out != None:
        for index, row in df.iterrows():
            if row["out_p_gis"] == line_out and row["check_in"] >= start_time and row["check_in"] < end_time:
                output_df = output_df.append(row)
                cntr += 1

    elif line_in != None and line_out == None:
        for index, row in df.iterrows():
            if row["in_p_gis"] == line_in and row["check_in"] >= start_time and row["check_in"] < end_time:
                output_df = output_df.append(row)
                cntr += 1

    elif line_in == None and line_out == None: #### in this case, only this condition works, other use if we want for example counting only destination trips, maybe in the future we will be interested in that
        for index, row in df.iterrows():
            if row["check_in"] >= start_time and row["check_in"] < end_time:
                output_df = output_df.append(row)
                cntr += 1
    elif line_in != None and line_out != None:
        for index, row in df.iterrows():
            if row["in_p_gis"] == line_in and row["out_p_gis"] == line_out and row["check_in"] >= start_time and row["check_in"] < end_time:
                output_df = output_df.append(row)
                cntr += 1
    return cntr, output_df
####################################
##########    Main      ############
####################################

if __name__ == "__main__":
    
    print(" Loading Data . . . ")
    smart_card_df = pd.read_csv('test.csv')
    # ####### converting time to HH:mm  to make it easy to work with time
    new_check_outs= []
    for index, row in smart_card_df.iterrows():
        if row['check_in'] >= row['check_out']:
            new_check_outs.append(row['check_out'] + 100)
        else:
            new_check_outs.append(row['check_out'])
    smart_card_df["check_out"] = new_check_outs
    smart_card_df["check_in"] = [int(row["check_in"]+2100) for idx, row in smart_card_df.iterrows()]
    smart_card_df["check_out"] = [int(row["check_out"]+2100) for idx, row in smart_card_df.iterrows()]
    smart_card_df["check_in"] = pd.to_datetime(smart_card_df["check_in"], format="%H%M")
    smart_card_df["check_out"] = pd.to_datetime(smart_card_df["check_out"], format="%H%M")
    # ########### distances
    distance_data = get_lines_distance(smart_card_df)
    print("K-Anonymity is loading ----------------")
    check_ins_corrected = divide_with_time_window(smart_card_df, time_col_name="check_in", station_name_col="in_p_gis", new_col_name = "id_in", time_window=10)
    check_outs_corrected = divide_with_time_window(smart_card_df, time_col_name="check_out", station_name_col="out_p_gis", new_col_name = "id_out", time_window=10)
    print(*check_ins_corrected, sep="\n-----------------\n\n\n")
    print(*check_outs_corrected, sep="\n-----------------\n\n\n")
    ###################################################
    ###### counting epochs travelers###################
    ###################################################
    step = 0
    neighbor = 0
    for index, check_in in zip(range(len(check_ins_corrected)), check_ins_corrected):
        counter_df_id_in = check_in.groupby('id_in', as_index=False).size()
        checkin_ids_dict = dict(zip(counter_df_id_in['id_in'], counter_df_id_in['size']))
        ####################
        new_dct_in = dict()
        for idin in checkin_ids_dict.keys():
            new_dct_in[idin] = list(check_in[check_in["id_in"] == idin]["new_id"])
        ####################
        check_out_idx = index + step
        neighbor_idx = check_out_idx + neighbor
        # print("\n****** STEP: {} ********\nLen Check_ins: {}\nLen Check_outs: {}\n**************\n".format(step, len(check_ins_corrected), len(check_outs_corrected)))
        if check_out_idx >= len(check_outs_corrected):
            check_out_idx = len(check_outs_corrected) - 1
        if neighbor_idx >= len(check_outs_corrected) or neighbor_idx < 0:
            neighbor = 0
        counter_df_id_out = check_outs_corrected[check_out_idx].groupby('id_out', as_index=False).size()
        checkout_ids_dict = dict(zip(counter_df_id_out['id_out'], counter_df_id_out['size']))
        ####################
        new_dct_out = dict()
        for idout in checkout_ids_dict.keys():
            new_dct_out[idout] = list(check_outs_corrected[check_out_idx][check_outs_corrected[check_out_idx]["id_out"] == idout]["new_id"])
        ####################
        if neighbor != 0:
            counter_df_id_nbr_out = check_outs_corrected[neighbor_idx].groupby('id_out', as_index=False).size()
            checkout_ids_nbr_dict = dict(zip(counter_df_id_nbr_out['id_out'], counter_df_id_nbr_out['size']))
            counter = collections.Counter()
            counter.update(checkout_ids_dict)
            counter.update(checkout_ids_nbr_dict)
            checkout_ids_dict = dict(counter)
        f1_score = dict()


        tru_p = 0
        false_p = 0
        true_n_list = []
        false_n = []
        all_ps = []
        all_ps_in_list = []
        all_ps_out_list =[]
        all_ps_in = []
        all_ps_out =[]

        dict_in_out_m = dict()
        ##### TN  ########
        all_ps_in = list(new_dct_in.values())
        all_ps_in_list = [m for h in all_ps_in for m in h]
        all_ps_out = list(new_dct_out.values())
        all_ps_out_list = [n for z in all_ps_out for n in z]

        for psy in all_ps_in_list:
            if psy not in all_ps_out_list:
                true_n_list.append(psy)
        dict_in_out_m = {**new_dct_in,**new_dct_out}### combining two dict
        common_keys =list(new_dct_in.keys() & new_dct_out.keys())### finding common key to remove them
        for key in common_keys:
            dict_in_out_m.pop(key, None)
        all_ps = list(dict_in_out_m.values())### all
        all_ps_list = [x for l in all_ps for x in l]
        print("++++++++++++++++++++++++++++++++++++++++++++++++")
        for k_in, k_val in zip(new_dct_in.keys(), new_dct_in.values()):
            if new_dct_out.get(k_in) != None:
                common_k = k_in
                tp = len(set(new_dct_out[k_in]) & set(k_val))
                fp = min(len(new_dct_out[k_in]), len(k_val)) - tp
                fn =list(set(new_dct_out[k_in])^set(k_val))
                false_n.append(fn)
                tru_p = tru_p + tp
                false_p = false_p + fp
                f1_score[k_in] = {"tp": tp, "fp": fp, "fn":fn }
        false_n = [x for p in false_n for x in p]
        for ps in false_n:
            all_ps_list.append(ps)
        fn_count = {i:all_ps_list.count(i) for i in all_ps_list}
        final_fn = len([k for k, v in fn_count.items() if v > 1])
        true_n = len(true_n_list)
        print("tru_P:,", tru_p)
        print("false_p:", false_p)
        print("final_fn:", final_fn)
        print("true_n:",true_n)
        ################## counting passengers################
        sum_psgr = 0
        for cin_key in checkin_ids_dict.keys():
            if checkout_ids_dict.get(cin_key) != None:
                sum_psgr += min(checkin_ids_dict.get(cin_key), checkout_ids_dict.get(cin_key))
        print("\n****** STEP: {} - iter: {} - neighbor: {} ********\nCheck-ins: \n{}\nCheck-outs: \n{}\n\n  Sum passengers: {}\n---------------------------------------\n".format(step, index+1, neighbor, checkin_ids_dict, checkout_ids_dict, sum_psgr))
    