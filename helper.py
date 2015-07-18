import stat
import os
import dataclasses
try:
    import xml.etree.cElementTree as ET
except ImportError:
    import xml.etree.ElementTree as ET

class c_HelperFunctions():
    def remove_readonly(self,fn, path, excinfo):
        try:
            os.chmod(path, stat.S_IWRITE)
            fn(path)
        except Exception as exc:
            print("Skipped:", path, "because:\n", exc)
    def indent(self, elem, level=0):
      i = "\n" + level*"  "
      if len(elem):
        if not elem.text or not elem.text.strip():
          elem.text = i + "  "
        if not elem.tail or not elem.tail.strip():
          elem.tail = i
        for elem in elem:
          self.indent(elem, level+1)
        if not elem.tail or not elem.tail.strip():
          elem.tail = i
      else:
        if level and (not elem.tail or not elem.tail.strip()):
          elem.tail = i
    def WriteJob(self,dict_workdata, dict_Jobs, ID):
        TaskObject = dict_Jobs[ID]
        Task = ET.Element("Task")
        Task.set("ID",str(ID))
        Task.set("state",str(TaskObject.state))
        Task.set("active",str(TaskObject.active))
        FileList = ET.SubElement(Task,"FileList")
        for file in TaskObject.filelist:
            data = TaskObject.filelist[file]
            fileItem = ET.SubElement(FileList,"File")
            fileItem.set("file",file)
            fileItem.set("copied",str(data.copied))
            fileItem.set("delete",str(data.delete))
            fileItem.set("uploaded",str(data.uploaded))
            fileItem.set("size",str(data.size))
        self.indent(Task)
        Tree = ET.ElementTree(Task)
        Tree.write(dict_workdata["sTargetDir"] + str(ID) + "/" + str(ID) + ".xml", xml_declaration=True, encoding='utf-8', method="xml")
    def get_filepaths(self,directory):
        """
        This function will generate the file names in a directory
        tree by walking the tree either top-down or bottom-up. For each
        directory in the tree rooted at directory top (including top itself),
        it yields a 3-tuple (dirpath, dirnames, filenames).
        """
        file_paths = []  # List which will store all of the full filepaths.

        # Walk the tree.
        for root, directories, files in os.walk(directory):
            for filename in files:
                # Join the two strings in order to form the full filepath.
                filepath = os.path.join(root, filename)
                file_paths.append(filepath)  # Add it to the list.

        return file_paths  # Self-explanatory.
    def FileExpand(self,ID,Payload):
        self.ID = ID
        self.Payload = Payload
        self.FileList = {}
        for FileObj in self.Payload:
            if FileObj["type"] == "folder":
                self.aFilePaths = self.get_filepaths(FileObj["data"])
                for f in self.aFilePaths:
                    self.path = os.path.normpath(f)
                    self.FileList[self.path] = dataclasses.c_file(os.path.getsize(self.path))
            elif FileObj["type"] == "file":
                self.path = os.path.normpath(FileObj["data"])
                self.FileList[self.path] = dataclasses.c_file(os.path.getsize(self.path))

        return self.FileList
