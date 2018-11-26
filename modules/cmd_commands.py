taskkill /f /im  python.exe





SELECT * FROM pg_stat_activity;

SELECT
  pg_terminate_backend(pid)
FROM
  pg_stat_activity
WHERE
 state = 'idle'


























# import numpy as np


# entirelist = [4, 4, 4, 2, 45, 37, 2, 61, 45, 37]
# traj_array = [1, 1, 1, 0, 2, 0, 0, 2, 2, 0]
# current_index=entirelist.index(61)

# beforelist=entirelist[:current_index]
# print 'beforelist---', beforelist

# ### remove the first for elements from traj_array
# traj_array_trunc = traj_array[:current_index]
# print 'traj_array_trunc---', traj_array_trunc


# # # import collections
# # # a = [1,1,1,1,2,2,2,2,3,3,4,5,5]
# # counter=collections.Counter(traj_array_trunc)
# # print(counter)

# afterlist=entirelist[current_index:]
# print 'afterlist---', afterlist


# def split(arr, cond):
#   return [arr[cond], arr[~cond]]



# a = np.array(traj_array_trunc)
# print np.diff(a)
