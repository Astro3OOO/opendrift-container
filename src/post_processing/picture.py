"""
    Trajectory picture
"""
def export_traj_picture(traj, file_name):
    file_name = file_name.replace('.nc', '.png')    
    traj.plot(filename = file_name)
    return